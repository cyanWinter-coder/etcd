// Copyright 2026 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package lease

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

// recordingDeleter is a TxnDelete that records deleted keys without taking the
// backend transaction lock, so it can be installed before the lessor is built
// (an eagerly built fakeDeleter would deadlock initAndRecover on the batch tx).
type recordingDeleter struct {
	mu      sync.Mutex
	deleted []string
}

func (d *recordingDeleter) DeleteRange(key, end []byte) (int64, int64) {
	d.mu.Lock()
	d.deleted = append(d.deleted, string(key)+"_"+string(end))
	d.mu.Unlock()
	return 0, 0
}

func (d *recordingDeleter) End() {}

func (d *recordingDeleter) snapshot() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.deleted...)
}

// newFakeClockLessor builds a lessor driven by a fake clock, with a range
// deleter attached so revoke exercises the same path as production.
func newFakeClockLessor(t *testing.T, clock clockwork.Clock, cb func(*LessorConfig)) (*lessor, *recordingDeleter, func()) {
	t.Helper()
	lg := zap.NewNop()
	dir, be := NewTestBackend(t)
	rd := &recordingDeleter{}

	cfg := LessorConfig{
		MinLeaseTTL: 1,
		clock:       clock,
	}
	if cb != nil {
		cb(&cfg)
	}
	le := newLessor(lg, be, clusterLatest(), cfg)
	le.SetRangeDeleter(func() TxnDelete { return rd })

	return le, rd, func() {
		le.Stop()
		be.Close()
		os.RemoveAll(dir)
	}
}

// TestLeaseExpiryOrderedByExpiry ensures the expiry queue hands leases back in
// expiry order regardless of grant order or renewal order, and that a renew
// re-queues the lease at its new expiry.
func TestLeaseExpiryOrderedByExpiry(t *testing.T) {
	fc := clockwork.NewFakeClockAt(time.Unix(1_700_000_000, 0))
	le, _, teardown := newFakeClockLessor(t, fc, nil)
	defer teardown()
	le.Promote(0)

	// Grant out of expiry order: 30s, 10s, 20s.
	grants := []struct {
		id  LeaseID
		ttl int64
	}{{1, 30}, {2, 10}, {3, 20}}
	for _, g := range grants {
		if _, err := le.Grant(g.id, g.ttl); err != nil {
			t.Fatalf("grant %d: %v", g.id, err)
		}
	}

	want := []LeaseID{2, 3, 1}
	for _, id := range want {
		got := le.leaseExpiredNotifier.Peek().id
		if got != id {
			t.Fatalf("head of expiry queue = %d, want %d", got, id)
		}
		le.leaseExpiredNotifier.Remove(got)
	}

	// Renewing lease 2 (the soonest to expire) must move it behind the rest.
	if _, err := le.Grant(4, 10); err != nil {
		t.Fatal(err)
	}
	if _, err := le.Grant(5, 20); err != nil {
		t.Fatal(err)
	}
	// At t+0: 4 expires at 10s, 5 at 20s.
	if _, err := le.Renew(4); err != nil {
		t.Fatalf("renew: %v", err)
	}
	// Lease 4 now expires at t+10s, same as before, but let the fake clock
	// move on so the renewed expiry is strictly later.
	fc.Advance(5 * time.Second)
	if _, err := le.Renew(4); err != nil {
		t.Fatalf("renew: %v", err)
	}
	// 4 now expires at t+15s (i.e. 10s after t+5), still before 5 (t+20s).
	if got := le.leaseExpiredNotifier.Peek().id; got != 4 {
		t.Fatalf("after renew head = %d, want 4", got)
	}
	fc.Advance(11 * time.Second)
	// 4 expires at t+15s; 5 at t+20s. Advancing past t+15s must expire 4 first.
	expired := le.findExpiredLeases(10)
	ids := make([]LeaseID, len(expired))
	for i, l := range expired {
		ids[i] = l.ID
	}
	if len(ids) != 1 || ids[0] != 4 {
		t.Fatalf("expired = %v, want [4]", ids)
	}
}

// TestLeaseExpiredBatchLimit ensures a bulk expiry is drained in bounded
// batches, and that repeated scans make progress.
func TestLeaseExpiredBatchLimit(t *testing.T) {
	fc := clockwork.NewFakeClockAt(time.Unix(1_700_000_000, 0))
	const batchLimit = 8
	le, _, teardown := newFakeClockLessor(t, fc, func(cfg *LessorConfig) {
		cfg.maxExpiredRevokeBatchSize = batchLimit
		cfg.leaseRevokeRate = 1000
	})
	defer teardown()
	le.Promote(0)

	const total = 50
	for i := 1; i <= total; i++ {
		if _, err := le.Grant(LeaseID(i), 5); err != nil {
			t.Fatal(err)
		}
	}
	// A clock jump expires everything at once.
	fc.Advance(time.Minute)

	le.mu.RLock()
	batch := le.findExpiredLeases(min(le.leaseRevokeRate/2, le.maxExpiredRevokeBatchSize))
	le.mu.RUnlock()
	if len(batch) != batchLimit {
		t.Fatalf("first batch = %d leases, want %d", len(batch), batchLimit)
	}

	// Drain the rest; each pass must make progress and never exceed the cap.
	seen := map[LeaseID]struct{}{}
	for _, l := range batch {
		seen[l.ID] = struct{}{}
	}
	for i := 0; i < 100 && len(seen) < total; i++ {
		le.mu.RLock()
		b := le.findExpiredLeases(batchLimit)
		le.mu.RUnlock()
		if len(b) > batchLimit {
			t.Fatalf("batch exceeded limit: %d", len(b))
		}
		for _, l := range b {
			seen[l.ID] = struct{}{}
			// Simulate the revoker deleting it so the queue drains.
			le.leaseExpiredNotifier.Remove(l.ID)
		}
	}
	if len(seen) != total {
		t.Fatalf("saw %d expired leases over all batches, want %d", len(seen), total)
	}
}

// TestLeaseClockRollbackDoesNotExpire ensures a backwards wall clock jump
// neither expires live leases early nor revives an already expired lease.
func TestLeaseClockRollbackDoesNotExpire(t *testing.T) {
	core := clockwork.NewFakeClockAt(time.Unix(1_700_000_000, 0))
	warned := make(chan time.Duration, 4)
	clock := newLeaseClock(core, time.Second, func(d time.Duration) { warned <- d })
	le, _, teardown := newFakeClockLessor(t, clock, nil)
	defer teardown()
	le.Promote(0)

	l, err := le.Grant(1, 30)
	if err != nil {
		t.Fatal(err)
	}
	expiryBefore := l.expiry

	// Jump an hour into the past.
	core.Advance(-time.Hour)
	if got := clock.Rollback(); got < time.Hour {
		t.Fatalf("rollback = %v, want at least an hour", got)
	}

	// The lease must still be live and must not be handed to the revoker.
	if l.expired() {
		t.Fatal("lease reported expired after backwards clock jump")
	}
	le.mu.RLock()
	expired := le.findExpiredLeases(100)
	le.mu.RUnlock()
	if len(expired) != 0 {
		t.Fatalf("revoker saw %d expired leases after clock rollback", len(expired))
	}

	// A renew during the rollback must not move the expiry backwards.
	if _, err := le.Renew(1); err != nil {
		t.Fatalf("renew during rollback: %v", err)
	}
	if l.expiry.Before(expiryBefore) {
		t.Fatalf("renew during rollback moved expiry backwards: %v -> %v", expiryBefore, l.expiry)
	}

	select {
	case d := <-warned:
		if d < time.Hour {
			t.Fatalf("warned rollback = %v, want at least an hour", d)
		}
	case <-time.After(time.Second):
		t.Fatal("large clock rollback did not trigger a warning")
	}
}

// TestLeaseClockRollbackWarningRateLimited ensures the warning fires when the
// rollback deepens rather than on every observation.
func TestLeaseClockRollbackWarningRateLimited(t *testing.T) {
	core := clockwork.NewFakeClockAt(time.Unix(1_700_000_000, 0))
	core.Now() // establish t0

	var mu sync.Mutex
	var warnings []time.Duration
	clock := newLeaseClock(core, time.Second, func(d time.Duration) {
		mu.Lock()
		warnings = append(warnings, d)
		mu.Unlock()
	})

	// Establish a high-water mark.
	clock.Now()
	core.Advance(-2 * time.Minute)
	for i := 0; i < 5; i++ {
		clock.Now()
	}
	mu.Lock()
	if len(warnings) != 1 {
		t.Fatalf("warnings = %v, want exactly one for a steady rollback", warnings)
	}
	mu.Unlock()

	// A deeper rollback should warn again.
	core.Advance(-time.Minute)
	clock.Now()
	mu.Lock()
	if len(warnings) != 2 {
		t.Fatalf("warnings = %v, want a second warning for a deeper rollback", warnings)
	}
	mu.Unlock()
}

// TestLeaseSmallClockDriftDoesNotWarn ensures sub-threshold corrections stay
// quiet.
func TestLeaseSmallClockDriftDoesNotWarn(t *testing.T) {
	core := clockwork.NewFakeClockAt(time.Unix(1_700_000_000, 0))
	warned := false
	clock := newLeaseClock(core, 5*time.Second, func(time.Duration) { warned = true })

	clock.Now()
	core.Advance(-100 * time.Millisecond)
	clock.Now()
	if warned {
		t.Fatal("small clock drift triggered a warning")
	}
}

// TestLeaseRepeatOperationsReturnExplicitErrors covers repeated operations on
// revoked leases and operations on expired ones.
func TestLeaseRepeatOperationsReturnExplicitErrors(t *testing.T) {
	fc := clockwork.NewFakeClockAt(time.Unix(1_700_000_000, 0))
	le, _, teardown := newFakeClockLessor(t, fc, nil)
	defer teardown()
	le.Promote(0)

	l, err := le.Grant(1, 30)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := le.Renew(l.ID); err != nil {
		t.Fatalf("renew: %v", err)
	}
	if err := le.Revoke(l.ID); err != nil {
		t.Fatalf("revoke: %v", err)
	}

	// Every repeated operation is reported as revoked, not as missing.
	if err := le.Revoke(l.ID); !errors.Is(err, ErrLeaseRevoked) {
		t.Errorf("second Revoke err = %v, want ErrLeaseRevoked", err)
	}
	if _, err := le.Renew(l.ID); !errors.Is(err, ErrLeaseRevoked) {
		t.Errorf("Renew after revoke err = %v, want ErrLeaseRevoked", err)
	}
	if err := le.Attach(l.ID, []LeaseItem{{Key: "k"}}); !errors.Is(err, ErrLeaseRevoked) {
		t.Errorf("Attach after revoke err = %v, want ErrLeaseRevoked", err)
	}
	if err := le.Detach(l.ID, []LeaseItem{{Key: "k"}}); !errors.Is(err, ErrLeaseRevoked) {
		t.Errorf("Detach after revoke err = %v, want ErrLeaseRevoked", err)
	}
	if !IsLeaseUnusable(ErrLeaseRevoked) {
		t.Error("ErrLeaseRevoked should be classified as unusable")
	}

	// A lease ID that never existed is still plain "not found".
	const unknown = LeaseID(4242)
	if err := le.Revoke(unknown); !errors.Is(err, ErrLeaseNotFound) {
		t.Errorf("Revoke unknown err = %v, want ErrLeaseNotFound", err)
	}
	if _, err := le.Renew(unknown); !errors.Is(err, ErrLeaseNotFound) {
		t.Errorf("Renew unknown err = %v, want ErrLeaseNotFound", err)
	}
}

// TestLeaseExpiredRenewUnblocksWithRevoked ensures a renew of a lapsed lease
// does not silently succeed: it waits for reclamation and then reports the
// lease as revoked.
func TestLeaseExpiredRenewUnblocksWithRevoked(t *testing.T) {
	fc := clockwork.NewFakeClockAt(time.Unix(1_700_000_000, 0))
	le, _, teardown := newFakeClockLessor(t, fc, nil)
	defer teardown()
	le.Promote(0)

	l, err := le.Grant(1, 5)
	if err != nil {
		t.Fatal(err)
	}
	fc.Advance(10 * time.Second)
	if !l.expired() {
		t.Fatal("lease should be expired")
	}

	errc := make(chan error, 1)
	go func() {
		_, err := le.Renew(l.ID)
		errc <- err
	}()
	select {
	case err := <-errc:
		t.Fatalf("Renew returned %v before the expired lease was reclaimed", err)
	case <-time.After(50 * time.Millisecond):
	}

	if err := le.Revoke(l.ID); err != nil {
		t.Fatalf("revoke expired lease: %v", err)
	}
	select {
	case err := <-errc:
		if !errors.Is(err, ErrLeaseRevoked) {
			t.Fatalf("Renew err = %v, want ErrLeaseRevoked", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Renew did not return after the lease was revoked")
	}
}

// TestLeaseExpiryDuringRenewReturnsExpired ensures a lease that lapses while
// its renewal is in flight (for example during a checkpoint round-trip) is
// reported as expired rather than being silently renewed.
func TestLeaseExpiryDuringRenewReturnsExpired(t *testing.T) {
	fc := clockwork.NewFakeClockAt(time.Unix(1_700_000_000, 0))
	le, _, teardown := newFakeClockLessor(t, fc, nil)
	defer teardown()
	le.Promote(0)

	l, err := le.Grant(1, 60)
	if err != nil {
		t.Fatal(err)
	}
	// A checkpointed remaining TTL makes Renew take the checkpointer path
	// before re-checking the lease.
	le.Checkpoint(l.ID, 10)
	le.SetCheckpointer(func(_ context.Context, cp *pb.LeaseCheckpointRequest) error {
		// The clock jumps past the expiry while the renew is in flight.
		fc.Advance(2 * time.Minute)
		for _, c := range cp.GetCheckpoints() {
			le.Checkpoint(LeaseID(c.GetID()), c.GetRemaining_TTL())
		}
		return nil
	})

	if _, err := le.Renew(l.ID); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("Renew err = %v, want ErrLeaseExpired", err)
	}
	if le.Lookup(l.ID) == nil {
		t.Fatal("lease should not have been removed by a renew")
	}
}

// TestLeaseRevokeConcurrentWithRenew ensures a revoke and a renew racing on the
// same lease leave consistent state and never panic (for example on a double
// close of revokec).
func TestLeaseRevokeConcurrentWithRenew(t *testing.T) {
	fc := clockwork.NewFakeClockAt(time.Unix(1_700_000_000, 0))
	le, _, teardown := newFakeClockLessor(t, fc, nil)
	defer teardown()
	le.Promote(0)

	for i := 0; i < 200; i++ {
		id := LeaseID(i + 1)
		if _, err := le.Grant(id, 30); err != nil {
			t.Fatal(err)
		}

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			le.Renew(id)
		}()
		go func() {
			defer wg.Done()
			le.Revoke(id)
		}()
		wg.Wait()

		if le.Lookup(id) != nil {
			t.Fatalf("lease %d still present after concurrent revoke", id)
		}
	}
}

// TestLeaseRepeatRevokeRejected ensures a duplicate concurrent revoke does not
// run the range delete twice.
func TestLeaseRepeatRevokeRejected(t *testing.T) {
	fc := clockwork.NewFakeClockAt(time.Unix(1_700_000_000, 0))
	le, fd, teardown := newFakeClockLessor(t, fc, nil)
	defer teardown()
	le.Promote(0)

	l, err := le.Grant(1, 30)
	if err != nil {
		t.Fatal(err)
	}
	if err := le.Attach(l.ID, []LeaseItem{{Key: "k"}}); err != nil {
		t.Fatal(err)
	}

	// Mark the lease as being revoked to emulate a concurrent in-flight
	// revoke, then attempt a second one.
	le.mu.Lock()
	l.revoking = true
	le.mu.Unlock()
	if err := le.Revoke(l.ID); !errors.Is(err, ErrLeaseRevoked) {
		t.Fatalf("concurrent revoke err = %v, want ErrLeaseRevoked", err)
	}
	if deleted := fd.snapshot(); len(deleted) != 0 {
		t.Fatalf("rejected revoke still deleted %v", deleted)
	}
}

// TestLeaseStats ensures the active and expiring counts stay readable while
// renewals and revokes run concurrently.
func TestLeaseStats(t *testing.T) {
	fc := clockwork.NewFakeClockAt(time.Unix(1_700_000_000, 0))
	le, _, teardown := newFakeClockLessor(t, fc, nil)
	defer teardown()
	le.Promote(0)

	const live, expiring = 20, 7
	for i := 1; i <= live; i++ {
		if _, err := le.Grant(LeaseID(i), 60); err != nil {
			t.Fatal(err)
		}
	}
	for i := live + 1; i <= live+expiring; i++ {
		if _, err := le.Grant(LeaseID(i), 5); err != nil {
			t.Fatal(err)
		}
	}

	if got := le.LeaseStats(); got.Active != live+expiring || got.Expiring != 0 {
		t.Fatalf("stats before expiry = %+v, want active=%d expiring=0", got, live+expiring)
	}

	fc.Advance(10 * time.Second)
	if got := le.LeaseStats(); got.Active != live+expiring || got.Expiring != expiring {
		t.Fatalf("stats after expiry = %+v, want active=%d expiring=%d", got, live+expiring, expiring)
	}

	// Revokes shrink the active count.
	if err := le.Revoke(live + 1); err != nil {
		t.Fatal(err)
	}
	if got := le.LeaseStats(); got.Active != live+expiring-1 {
		t.Fatalf("stats after revoke = %+v", got)
	}

	// Stats must be safe to read concurrently with renew and revoke.
	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				st := le.LeaseStats()
				if st.Expiring < 0 || st.Expiring > st.Active {
					t.Errorf("inconsistent stats: %+v", st)
					return
				}
			}
		}
	}()
	for i := 1; i <= live; i++ {
		le.Renew(LeaseID(i))
	}
	for i := 1; i <= live; i++ {
		if err := le.Revoke(LeaseID(i)); err != nil {
			t.Fatalf("revoke %d: %v", i, err)
		}
	}
	close(stop)
	wg.Wait()
}

// TestLeaseQueueStableTieBreak ensures equal expiry times are ordered
// deterministically by lease ID.
func TestLeaseQueueStableTieBreak(t *testing.T) {
	n := newLeaseExpiredNotifier()
	ts := time.Unix(1_700_000_000, 0)
	for _, id := range []LeaseID{5, 3, 9, 1, 7} {
		n.RegisterOrUpdate(&LeaseWithTime{id: id, time: ts})
	}
	want := []LeaseID{1, 3, 5, 7, 9}
	for _, w := range want {
		if got := n.Unregister().id; got != w {
			t.Fatalf("got %d, want %d", got, w)
		}
	}
}

// TestLeaseRollbackWarnsThroughLessor ensures the lessor itself (not just the
// clock wrapper) emits the rollback warning through its logger.
func TestLeaseRollbackWarnsThroughLessor(t *testing.T) {
	core := clockwork.NewFakeClockAt(time.Unix(1_700_000_000, 0))
	coreObserved, logs := observer.New(zap.WarnLevel)

	dir, be := NewTestBackend(t)
	defer os.RemoveAll(dir)
	defer be.Close()
	le := newLessor(zap.New(coreObserved), be, clusterLatest(), LessorConfig{
		MinLeaseTTL:                1,
		clock:                      core,
		clockRollbackWarnThreshold: 30 * time.Second,
	})
	defer le.Stop()
	le.Promote(0)
	if _, err := le.Grant(1, 60); err != nil {
		t.Fatal(err)
	}

	core.Advance(-time.Hour)
	le.now()

	if logs.FilterMessageSnippet("wall clock moved backwards").Len() == 0 {
		t.Fatal("no rollback warning was logged")
	}
}
