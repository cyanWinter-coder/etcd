// Copyright 2015 The etcd Authors
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
	"container/heap"
	"context"
	"errors"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/Masterminds/semver/v3"
	"github.com/jonboulle/clockwork"
	"go.uber.org/zap"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/version"
	"go.etcd.io/etcd/server/v3/lease/leasepb"
	"go.etcd.io/etcd/server/v3/storage/backend"
	"go.etcd.io/etcd/server/v3/storage/schema"
)

// NoLease is a special LeaseID representing the absence of a lease.
const NoLease = LeaseID(0)

// MaxLeaseTTL is the maximum lease TTL value
const MaxLeaseTTL = 9000000000

var (
	forever = time.Time{}

	// default number of leases to revoke per second; configurable for tests
	defaultLeaseRevokeRate = 1000

	// maximum number of lease checkpoints recorded to the consensus log per second; configurable for tests
	leaseCheckpointRate = 1000

	// the default interval of lease checkpoint
	defaultLeaseCheckpointInterval = 5 * time.Minute

	// maximum number of lease checkpoints to batch into a single consensus log entry
	maxLeaseCheckpointBatchSize = 1000

	// the default interval to check if the expired lease is revoked
	defaultExpiredleaseRetryInterval = 3 * time.Second

	// maximum number of expired leases handed to the revoker in a single
	// batch. Bounds the work the expired-lease channel puts on a follower or
	// leader in one scheduling round; larger expirations are drained over
	// subsequent rounds instead of in one burst.
	defaultMaxExpiredRevokeBatchSize = 1000

	// by how much the wall clock has to jump backwards before the lessor
	// reports it. Small forward/backward corrections are absorbed by the
	// monotonic lease clock without noise.
	defaultLeaseClockRollbackWarnThreshold = 5 * time.Second

	// how many recently revoked lease IDs are remembered to distinguish
	// repeated operations on a revoked lease from an unknown lease ID.
	revokedLeaseMemorySize = 1 << 16

	ErrNotPrimary       = errors.New("not a primary lessor")
	ErrLeaseNotFound    = errors.New("lease not found")
	ErrLeaseExists      = errors.New("lease already exists")
	ErrLeaseTTLTooLarge = errors.New("too large lease TTL")
	// ErrLeaseRevoked is returned when an operation targets a lease that is
	// already being revoked, or was recently revoked.
	ErrLeaseRevoked = errors.New("lease already revoked")
	// ErrLeaseExpired is returned when an operation targets a lease that is
	// past its expiry but has not been reclaimed yet.
	ErrLeaseExpired = errors.New("lease already expired")
)

// IsLeaseUnusable reports whether err means the referenced lease can no longer
// be used (it was never granted, has been revoked, or has expired). Callers at
// the API boundary treat all of these as "lease not found".
func IsLeaseUnusable(err error) bool {
	return errors.Is(err, ErrLeaseNotFound) ||
		errors.Is(err, ErrLeaseRevoked) ||
		errors.Is(err, ErrLeaseExpired)
}

// TxnDelete is a TxnWrite that only permits deletes. Defined here
// to avoid circular dependency with mvcc.
type TxnDelete interface {
	DeleteRange(key, end []byte) (n, rev int64)
	End()
}

// RangeDeleter is a TxnDelete constructor.
type RangeDeleter func() TxnDelete

// Checkpointer permits checkpointing of lease remaining TTLs to the consensus log. Defined here to
// avoid circular dependency with mvcc.
type Checkpointer func(ctx context.Context, lc *pb.LeaseCheckpointRequest) error

type LeaseID int64

// Lessor owns leases. It can grant, revoke, renew and modify leases for lessee.
type Lessor interface {
	// SetRangeDeleter lets the lessor create TxnDeletes to the store.
	// Lessor deletes the items in the revoked or expired lease by creating
	// new TxnDeletes.
	SetRangeDeleter(rd RangeDeleter)

	SetCheckpointer(cp Checkpointer)

	// Grant grants a lease that expires at least after TTL seconds.
	Grant(id LeaseID, ttl int64) (*Lease, error)
	// Revoke revokes a lease with given ID. The item attached to the
	// given lease will be removed. If the ID does not exist, ErrLeaseNotFound
	// is returned; if it was already revoked, ErrLeaseRevoked is returned.
	Revoke(id LeaseID) error

	// Checkpoint applies the remainingTTL of a lease. The remainingTTL is used in Promote to set
	// the expiry of leases to less than the full TTL when possible.
	Checkpoint(id LeaseID, remainingTTL int64) error

	// Attach attaches given leaseItem to the lease with given LeaseID.
	// If the lease does not exist, an error will be returned.
	Attach(id LeaseID, items []LeaseItem) error

	// GetLease returns LeaseID for given item.
	// If no lease found, NoLease value will be returned.
	GetLease(item LeaseItem) LeaseID

	// Detach detaches given leaseItem from the lease with given LeaseID.
	// If the lease does not exist, an error will be returned.
	Detach(id LeaseID, items []LeaseItem) error

	// Promote promotes the lessor to be the primary lessor. Primary lessor manages
	// the expiration and renew of leases.
	// Newly promoted lessor renew the TTL of all lease to extend + previous TTL.
	Promote(extend time.Duration)

	// Demote demotes the lessor from being the primary lessor.
	Demote()

	// Renew renews a lease with given ID. It returns the renewed TTL. If the ID does not
	// exist, has been revoked, or has expired, an error will be returned.
	Renew(id LeaseID) (int64, error)

	// Lookup gives the lease at a given lease id, if any
	Lookup(id LeaseID) *Lease

	// Leases lists all leases.
	Leases() []*Lease

	// ExpiredLeasesC returns a chan that is used to receive expired leases.
	ExpiredLeasesC() <-chan []*Lease

	// LeaseStats returns a point-in-time summary of the managed leases: the
	// number of live leases, and how many of them are already past their
	// expiry but not yet reclaimed.
	LeaseStats() LeaseStats

	// Recover recovers the lessor state from the given backend and RangeDeleter.
	Recover(b backend.Backend, rd RangeDeleter)

	// Stop stops the lessor for managing leases. The behavior of calling Stop multiple
	// times is undefined.
	Stop()
}

// LeaseStats summarizes the state of the leases owned by a lessor.
type LeaseStats struct {
	// Active is the number of leases currently held by the lessor, including
	// those that are already past their expiry but not yet reclaimed.
	Active int
	// Expiring is the number of leases whose expiry has passed but which have
	// not been revoked yet. It is a subset of Active.
	Expiring int
}

// lessor implements Lessor interface.
type lessor struct {
	mu sync.RWMutex

	// demotec is set when the lessor is the primary.
	// demotec will be closed if the lessor is demoted.
	demotec chan struct{}

	leaseMap             map[LeaseID]*Lease
	leaseExpiredNotifier *LeaseExpiredNotifier
	leaseCheckpointHeap  LeaseQueue
	itemMap              map[LeaseItem]LeaseID

	// recentlyRevoked remembers a bounded window of revoked lease IDs, so
	// repeated operations can return ErrLeaseRevoked rather than "not found".
	recentlyRevoked *revokedLeases

	// clock is the monotonic lease clock; it never moves backwards even if
	// the wall clock does. All expiry arithmetic goes through it.
	clock *leaseClock

	// When a lease expires, the lessor will delete the
	// leased range (or key) by the RangeDeleter.
	rd RangeDeleter

	// When a lease's deadline should be persisted to preserve the remaining TTL across leader
	// elections and restarts, the lessor will checkpoint the lease by the Checkpointer.
	cp Checkpointer

	// backend to persist leases. We only persist lease ID and expiry for now.
	// The leased items can be recovered by iterating all the keys in kv.
	b backend.Backend

	// minLeaseTTL is the minimum lease TTL that can be granted for a lease. Any
	// requests for shorter TTLs are extended to the minimum TTL.
	minLeaseTTL int64

	// maximum number of leases to revoke per second
	leaseRevokeRate int
	// maximum number of expired leases returned in a single batch
	maxExpiredRevokeBatchSize int

	expiredC chan []*Lease
	// stopC is a channel whose closure indicates that the lessor should be stopped.
	stopC chan struct{}
	// doneC is a channel whose closure indicates that the lessor is stopped.
	doneC chan struct{}

	lg *zap.Logger

	// Wait duration between lease checkpoints.
	checkpointInterval time.Duration
	// the interval to check if the expired lease is revoked
	expiredLeaseRetryInterval time.Duration
	// whether lessor should always persist remaining TTL (always enabled in v3.6).
	checkpointPersist bool
	// cluster is used to adapt lessor logic based on cluster version
	cluster cluster
}

type cluster interface {
	// Version is the cluster-wide minimum major.minor version.
	Version() *semver.Version
}

type LessorConfig struct {
	MinLeaseTTL                int64
	CheckpointInterval         time.Duration
	ExpiredLeasesRetryInterval time.Duration
	CheckpointPersist          bool

	leaseRevokeRate int
	// maxExpiredRevokeBatchSize caps how many expired leases are handed to
	// the revoker per batch. Zero means defaultMaxExpiredRevokeBatchSize.
	maxExpiredRevokeBatchSize int
	// clock is the clock used for expiry arithmetic. It may be a fake clock
	// for tests; nil means the wall clock.
	clock clockwork.Clock
	// clockRollbackWarnThreshold is how far the wall clock must jump
	// backwards before the lessor emits a warning. Zero means
	// defaultLeaseClockRollbackWarnThreshold; a negative value disables the
	// warning.
	clockRollbackWarnThreshold time.Duration
}

func NewLessor(lg *zap.Logger, b backend.Backend, cluster cluster, cfg LessorConfig) Lessor {
	return newLessor(lg, b, cluster, cfg)
}

func newLessor(lg *zap.Logger, b backend.Backend, cluster cluster, cfg LessorConfig) *lessor {
	checkpointInterval := cfg.CheckpointInterval
	expiredLeaseRetryInterval := cfg.ExpiredLeasesRetryInterval
	leaseRevokeRate := cfg.leaseRevokeRate
	maxExpiredRevokeBatchSize := cfg.maxExpiredRevokeBatchSize
	if checkpointInterval == 0 {
		checkpointInterval = defaultLeaseCheckpointInterval
	}
	if expiredLeaseRetryInterval == 0 {
		expiredLeaseRetryInterval = defaultExpiredleaseRetryInterval
	}
	if leaseRevokeRate == 0 {
		leaseRevokeRate = defaultLeaseRevokeRate
	}
	if maxExpiredRevokeBatchSize <= 0 {
		maxExpiredRevokeBatchSize = defaultMaxExpiredRevokeBatchSize
	}
	clockRollbackWarnThreshold := cfg.clockRollbackWarnThreshold
	if clockRollbackWarnThreshold == 0 {
		clockRollbackWarnThreshold = defaultLeaseClockRollbackWarnThreshold
	}
	clock := newLeaseClock(cfg.clock, clockRollbackWarnThreshold, func(rollback time.Duration) {
		if lg != nil {
			lg.Warn(
				"wall clock moved backwards; lease expiry is using the last known time until the clock catches up",
				zap.Duration("rollback", rollback),
				zap.Duration("warn-threshold", clockRollbackWarnThreshold),
			)
		}
	})
	l := &lessor{
		leaseMap:                  make(map[LeaseID]*Lease),
		itemMap:                   make(map[LeaseItem]LeaseID),
		leaseExpiredNotifier:      newLeaseExpiredNotifier(),
		leaseCheckpointHeap:       make(LeaseQueue, 0),
		recentlyRevoked:           newRevokedLeases(revokedLeaseMemorySize),
		clock:                     clock,
		b:                         b,
		minLeaseTTL:               cfg.MinLeaseTTL,
		leaseRevokeRate:           leaseRevokeRate,
		maxExpiredRevokeBatchSize: maxExpiredRevokeBatchSize,
		checkpointInterval:        checkpointInterval,
		expiredLeaseRetryInterval: expiredLeaseRetryInterval,
		checkpointPersist:         cfg.CheckpointPersist,
		// expiredC is a small buffered chan to avoid unnecessary blocking.
		expiredC: make(chan []*Lease, 16),
		stopC:    make(chan struct{}),
		doneC:    make(chan struct{}),
		lg:       lg,
		cluster:  cluster,
	}
	l.initAndRecover()

	go l.runLoop()

	return l
}

// isPrimary indicates if this lessor is the primary lessor. The primary
// lessor manages lease expiration and renew.
//
// in etcd, raft leader is the primary. Thus there might be two primary
// leaders at the same time (raft allows concurrent leader but with different term)
// for at most a leader election timeout.
// The old primary leader cannot affect the correctness since its proposal has a
// smaller term and will not be committed.
//
// TODO: raft follower do not forward lease management proposals. There might be a
// very small window (within second normally which depends on go scheduling) that
// a raft follow is the primary between the raft leader demotion and lessor demotion.
// Usually this should not be a problem. Lease should not be that sensitive to timing.
func (le *lessor) isPrimary() bool {
	return le.demotec != nil
}

// now returns the current time from the lease clock. A nil clock (as in tests
// that build a lessor literal by hand) falls back to the wall clock.
func (le *lessor) now() time.Time {
	if le.clock == nil {
		return time.Now()
	}
	return le.clock.Now()
}

func (le *lessor) SetRangeDeleter(rd RangeDeleter) {
	le.mu.Lock()
	defer le.mu.Unlock()

	le.rd = rd
}

func (le *lessor) SetCheckpointer(cp Checkpointer) {
	le.mu.Lock()
	defer le.mu.Unlock()

	le.cp = cp
}

func (le *lessor) Grant(id LeaseID, ttl int64) (*Lease, error) {
	if id == NoLease {
		return nil, ErrLeaseNotFound
	}

	if ttl > MaxLeaseTTL {
		return nil, ErrLeaseTTLTooLarge
	}

	// TODO: when lessor is under high load, it should give out lease
	// with longer TTL to reduce renew load.
	l := NewLease(id, ttl)

	le.mu.Lock()
	defer le.mu.Unlock()

	if _, ok := le.leaseMap[id]; ok {
		return nil, ErrLeaseExists
	}

	if l.ttl < le.minLeaseTTL {
		l.ttl = le.minLeaseTTL
	}
	l.clock = le.clock

	if le.isPrimary() {
		l.refresh(0)
	} else {
		l.forever()
	}

	le.leaseMap[id] = l
	l.persistTo(le.b)

	leaseTotalTTLs.Observe(float64(l.ttl))
	leaseGranted.Inc()

	if le.isPrimary() {
		item := &LeaseWithTime{id: l.ID, time: l.expiry}
		le.leaseExpiredNotifier.RegisterOrUpdate(item)
		le.scheduleCheckpointIfNeeded(l)
	}

	return l, nil
}

func (le *lessor) Revoke(id LeaseID) error {
	le.mu.Lock()

	l := le.leaseMap[id]
	if l == nil {
		err := le.leaseUnavailableLocked(id)
		le.mu.Unlock()
		return err
	}
	if l.revoking {
		// Another revoke for the same lease is already in flight. Rejecting
		// the duplicate keeps the external work (and the persistent delete)
		// from running twice for one lease.
		le.mu.Unlock()
		return ErrLeaseRevoked
	}
	l.revoking = true
	// Remove the lease from the expiry queue right away. Otherwise the
	// revoker could hand it out again while the delete is still in flight.
	le.leaseExpiredNotifier.Remove(id)

	// unlock before doing external work
	le.mu.Unlock()

	// Closing revokec unblocks Renew callers waiting on an expired lease.
	// revokeOnce makes this safe even if two revokes raced past the check
	// above (for example with older code paths).
	l.revokeOnce.Do(func() { close(l.revokec) })

	if le.rd == nil {
		// No store is attached yet: drop the in-memory lease only.
		le.finishRevoke(l, false)
		return nil
	}

	txn := le.rd()

	// sort keys so deletes are in same order among all members,
	// otherwise the backend hashes will be different
	keys := l.Keys()
	sort.StringSlice(keys).Sort()
	for _, key := range keys {
		txn.DeleteRange([]byte(key), nil)
	}

	// lease deletion needs to be in the same backend transaction with the
	// kv deletion. Or we might end up with not executing the revoke or not
	// deleting the keys if etcdserver fails in between. txn holds the backend
	// write lock, so the lease bucket write below is safe.
	le.finishRevoke(l, true)

	txn.End()

	leaseRevoked.Inc()
	return nil
}

// finishRevoke removes the lease from the in-memory state, optionally persists
// the deletion, and records the lease as recently revoked.
func (le *lessor) finishRevoke(l *Lease, persist bool) {
	le.mu.Lock()
	defer le.mu.Unlock()

	if le.leaseMap[l.ID] != l {
		// Another path already removed it.
		return
	}
	delete(le.leaseMap, l.ID)
	if persist {
		schema.UnsafeDeleteLease(le.b.BatchTx(), &leasepb.Lease{ID: int64(l.ID)})
	}
	le.recentlyRevoked.add(l.ID)
}

func (le *lessor) Checkpoint(id LeaseID, remainingTTL int64) error {
	le.mu.Lock()
	defer le.mu.Unlock()

	if l, ok := le.leaseMap[id]; ok {
		// when checkpointing, we only update the remainingTTL, Promote is responsible for applying this to lease expiry
		l.remainingTTL = remainingTTL
		if le.shouldPersistCheckpoints() {
			l.persistTo(le.b)
		}
		if le.isPrimary() {
			// schedule the next checkpoint as needed
			le.scheduleCheckpointIfNeeded(l)
		}
	}
	return nil
}

func (le *lessor) shouldPersistCheckpoints() bool {
	cv := le.cluster.Version()
	return le.checkpointPersist || (cv != nil && greaterOrEqual(*cv, version.V3_6))
}

func greaterOrEqual(first, second semver.Version) bool {
	return !version.LessThan(first, second)
}

// Renew renews an existing lease. If the given lease does not exist, has been
// revoked, or has expired, an error will be returned.
func (le *lessor) Renew(id LeaseID) (int64, error) {
	le.mu.RLock()
	if !le.isPrimary() {
		// forward renew request to primary instead of returning error.
		le.mu.RUnlock()
		return -1, ErrNotPrimary
	}

	demotec := le.demotec

	l := le.leaseMap[id]
	if l == nil {
		err := le.leaseUnavailableLocked(id)
		le.mu.RUnlock()
		return -1, err
	}
	if l.revoking {
		le.mu.RUnlock()
		return -1, ErrLeaseRevoked
	}
	// Clear remaining TTL when we renew if it is set
	clearRemainingTTL := le.cp != nil && l.remainingTTL > 0

	le.mu.RUnlock()
	if l.expired() {
		select {
		// A expired lease might be pending for revoking or going through
		// quorum to be revoked. To be accurate, renew request must wait for the
		// deletion to complete.
		case <-l.revokec:
			return -1, ErrLeaseRevoked
		// The expired lease might fail to be revoked if the primary changes.
		// The caller will retry on ErrNotPrimary.
		case <-demotec:
			return -1, ErrNotPrimary
		case <-le.stopC:
			return -1, ErrNotPrimary
		}
	}

	// gofail: var beforeCheckpointInLeaseRenew struct{}

	// Clear remaining TTL when we renew if it is set
	// By applying a RAFT entry only when the remainingTTL is already set, we limit the number
	// of RAFT entries written per lease to a max of 2 per checkpoint interval.
	if clearRemainingTTL {
		if err := le.cp(context.Background(), &pb.LeaseCheckpointRequest{Checkpoints: []*pb.LeaseCheckpoint{{ID: int64(l.ID), Remaining_TTL: 0}}}); err != nil {
			return -1, err
		}
	}

	le.mu.Lock()
	// Re-check in case the lease was revoked immediately after the previous check
	l = le.leaseMap[id]
	if l == nil {
		err := le.leaseUnavailableLocked(id)
		le.mu.Unlock()
		return -1, err
	}
	// A lease that expired while the checkpointer was running must not be
	// revived: the revoker may already be deleting its keys.
	if l.expired() {
		le.mu.Unlock()
		return -1, ErrLeaseExpired
	}
	l.refresh(0)
	// Renew puts the lease back into the expiry queue ordered by its new
	// expiry, so the soonest-expiring lease stays at the head.
	if !le.leaseExpiredNotifier.Update(l.ID, l.expiry) {
		// The lease was not queued (for example it was missed by a prior
		// queue reset). Register it rather than leave it without an expiry
		// trigger, which would leak it until the next promote.
		le.leaseExpiredNotifier.RegisterOrUpdate(&LeaseWithTime{id: l.ID, time: l.expiry})
	}
	le.mu.Unlock()

	leaseRenewed.Inc()
	return l.ttl, nil
}

func (le *lessor) Lookup(id LeaseID) *Lease {
	le.mu.RLock()
	defer le.mu.RUnlock()
	return le.leaseMap[id]
}

func (le *lessor) unsafeLeases() []*Lease {
	leases := make([]*Lease, 0, len(le.leaseMap))
	for _, l := range le.leaseMap {
		leases = append(leases, l)
	}
	return leases
}

func (le *lessor) Leases() []*Lease {
	le.mu.RLock()
	ls := le.unsafeLeases()
	le.mu.RUnlock()
	sort.Sort(leasesByExpiry(ls))
	return ls
}

// LeaseStats reports how many leases are held and how many of them are past
// their expiry but not yet reclaimed. A non-primary lessor keeps every lease
// at a forever expiry, so it reports Expiring as zero.
func (le *lessor) LeaseStats() LeaseStats {
	le.mu.RLock()
	defer le.mu.RUnlock()
	return le.leaseStatsLocked()
}

// leaseStatsLocked computes the stats assuming le.mu is held.
func (le *lessor) leaseStatsLocked() LeaseStats {
	stats := LeaseStats{Active: len(le.leaseMap)}
	if !le.isPrimary() {
		return stats
	}
	for _, l := range le.leaseMap {
		if l.expired() {
			stats.Expiring++
		}
	}
	return stats
}

func (le *lessor) Promote(extend time.Duration) {
	le.mu.Lock()
	defer le.mu.Unlock()

	le.demotec = make(chan struct{})

	// refresh the expiries of all leases.
	for _, l := range le.leaseMap {
		l.refresh(extend)
		item := &LeaseWithTime{id: l.ID, time: l.expiry}
		le.leaseExpiredNotifier.RegisterOrUpdate(item)
		le.scheduleCheckpointIfNeeded(l)
	}

	if len(le.leaseMap) < le.leaseRevokeRate {
		// no possibility of lease pile-up
		return
	}

	// adjust expiries in case of overlap
	leases := le.unsafeLeases()
	sort.Sort(leasesByExpiry(leases))

	baseWindow := leases[0].Remaining()
	nextWindow := baseWindow + time.Second
	expires := 0
	// have fewer expires than the total revoke rate so piled up leases
	// don't consume the entire revoke limit
	targetExpiresPerSecond := (3 * le.leaseRevokeRate) / 4
	for _, l := range leases {
		remaining := l.Remaining()
		if remaining > nextWindow {
			baseWindow = remaining
			nextWindow = baseWindow + time.Second
			expires = 1
			continue
		}
		expires++
		if expires <= targetExpiresPerSecond {
			continue
		}
		rateDelay := float64(time.Second) * (float64(expires) / float64(targetExpiresPerSecond))
		// If leases are extended by n seconds, leases n seconds ahead of the
		// base window should be extended by only one second.
		rateDelay -= float64(remaining - baseWindow)
		delay := time.Duration(rateDelay)
		nextWindow = baseWindow + delay
		l.refresh(delay + extend)
		item := &LeaseWithTime{id: l.ID, time: l.expiry}
		le.leaseExpiredNotifier.RegisterOrUpdate(item)
		le.scheduleCheckpointIfNeeded(l)
	}
}

func (le *lessor) Demote() {
	le.mu.Lock()
	defer le.mu.Unlock()

	// set the expiries of all leases to forever
	for _, l := range le.leaseMap {
		l.forever()
	}

	le.clearScheduledLeasesCheckpoints()
	le.clearLeaseExpiredNotifier()

	if le.demotec != nil {
		close(le.demotec)
		le.demotec = nil
	}
}

// Attach attaches items to the lease with given ID. When the lease
// expires, the attached items will be automatically removed.
// If the given lease does not exist, an error will be returned.
func (le *lessor) Attach(id LeaseID, items []LeaseItem) error {
	le.mu.Lock()
	defer le.mu.Unlock()

	l := le.leaseMap[id]
	if l == nil {
		return le.leaseUnavailableLocked(id)
	}
	if l.revoking {
		return ErrLeaseRevoked
	}

	l.mu.Lock()
	for _, it := range items {
		l.itemSet[it] = struct{}{}
		le.itemMap[it] = id
	}
	l.mu.Unlock()
	return nil
}

// leaseUnavailableLocked explains why a lease lookup came back empty. A lease
// that was recently revoked is reported as revoked, so a repeat of an
// operation that already succeeded fails with ErrLeaseRevoked instead of the
// misleading ErrLeaseNotFound. The caller must hold le.mu.
func (le *lessor) leaseUnavailableLocked(id LeaseID) error {
	if le.recentlyRevoked.contains(id) {
		return ErrLeaseRevoked
	}
	return ErrLeaseNotFound
}

func (le *lessor) GetLease(item LeaseItem) LeaseID {
	le.mu.RLock()
	id := le.itemMap[item]
	le.mu.RUnlock()
	return id
}

// Detach detaches items from the lease with given ID.
// If the given lease does not exist, an error will be returned.
func (le *lessor) Detach(id LeaseID, items []LeaseItem) error {
	le.mu.Lock()
	defer le.mu.Unlock()

	l := le.leaseMap[id]
	if l == nil {
		return le.leaseUnavailableLocked(id)
	}

	l.mu.Lock()
	for _, it := range items {
		delete(l.itemSet, it)
		delete(le.itemMap, it)
	}
	l.mu.Unlock()
	return nil
}

func (le *lessor) Recover(b backend.Backend, rd RangeDeleter) {
	le.mu.Lock()
	defer le.mu.Unlock()

	le.b = b
	le.rd = rd
	le.leaseMap = make(map[LeaseID]*Lease)
	le.itemMap = make(map[LeaseItem]LeaseID)
	le.initAndRecover()
}

func (le *lessor) ExpiredLeasesC() <-chan []*Lease {
	return le.expiredC
}

func (le *lessor) Stop() {
	close(le.stopC)
	<-le.doneC
}

func (le *lessor) runLoop() {
	defer close(le.doneC)

	// The scan ticker runs on the wall clock: a backwards clock jump must not
	// stall the loop's ability to revisit the expiry queue.
	delayTicker := time.NewTicker(500 * time.Millisecond)
	defer delayTicker.Stop()

	for {
		le.revokeExpiredLeases()
		le.checkpointScheduledLeases()

		select {
		case <-delayTicker.C:
		case <-le.stopC:
			return
		}
	}
}

// revokeExpiredLeases finds all leases past their expiry and sends them to expired channel for
// to be revoked.
func (le *lessor) revokeExpiredLeases() {
	var ls []*Lease

	// rate limit per round, and cap the batch handed to the revoker so a
	// clock jump that expires a large set at once is drained over several
	// rounds instead of in one burst.
	revokeLimit := le.leaseRevokeRate / 2
	if revokeLimit > le.maxExpiredRevokeBatchSize {
		revokeLimit = le.maxExpiredRevokeBatchSize
	}
	if revokeLimit <= 0 {
		revokeLimit = 1
	}

	le.mu.RLock()
	if le.isPrimary() {
		ls = le.findExpiredLeases(revokeLimit)
		stats := le.leaseStatsLocked()
		leaseActiveLeases.Set(float64(stats.Active))
		leaseExpiringLeases.Set(float64(stats.Expiring))
	}
	le.mu.RUnlock()

	if len(ls) != 0 {
		select {
		case <-le.stopC:
			return
		case le.expiredC <- ls:
		default:
			// the receiver of expiredC is probably busy handling
			// other stuff
			// let's try this next time after 500ms
		}
	}
}

// checkpointScheduledLeases finds all scheduled lease checkpoints that are due and
// submits them to the checkpointer to persist them to the consensus log.
func (le *lessor) checkpointScheduledLeases() {
	// rate limit
	for i := 0; i < leaseCheckpointRate/2; i++ {
		var cps []*pb.LeaseCheckpoint

		le.mu.Lock()
		if le.isPrimary() {
			cps = le.findDueScheduledCheckpoints(maxLeaseCheckpointBatchSize)
		}
		le.mu.Unlock()

		if len(cps) != 0 {
			if err := le.cp(context.Background(), &pb.LeaseCheckpointRequest{Checkpoints: cps}); err != nil {
				return
			}
		}
		if len(cps) < maxLeaseCheckpointBatchSize {
			return
		}
	}
}

func (le *lessor) clearScheduledLeasesCheckpoints() {
	le.leaseCheckpointHeap = make(LeaseQueue, 0)
}

func (le *lessor) clearLeaseExpiredNotifier() {
	le.leaseExpiredNotifier = newLeaseExpiredNotifier()
}

// expireExists returns "l" which is not nil if expiry items exist.
// It pops only when expiry item exists.
// "next" is true, to indicate that it may exist in next attempt.
func (le *lessor) expireExists() (l *Lease, next bool) {
	if le.leaseExpiredNotifier.Len() == 0 {
		return nil, false
	}

	item := le.leaseExpiredNotifier.Peek()
	l = le.leaseMap[item.id]
	if l == nil {
		// lease has expired or been revoked
		// no need to revoke (nothing is expiry)
		le.leaseExpiredNotifier.Unregister() // O(log N)
		return nil, true
	}
	now := le.now()
	if now.Before(item.time) /* item.time: expiration time */ {
		// Candidate expirations are caught up, reinsert this item
		// and no need to revoke (nothing is expiry)
		return nil, false
	}

	// recheck if revoke is complete after retry interval
	item.time = now.Add(le.expiredLeaseRetryInterval)
	le.leaseExpiredNotifier.RegisterOrUpdate(item)
	return l, false
}

// findExpiredLeases loops leases in the leaseMap until reaching expired limit
// and returns the expired leases that needed to be revoked.
func (le *lessor) findExpiredLeases(limit int) []*Lease {
	leases := make([]*Lease, 0, 16)

	// Count the never-expiring queue entries visited in this pass. If the
	// queue is full of leases that are not (or no longer) expired, the retry
	// interval rewrites their heap time to now+interval, so only a bounded
	// number of them can be at the head. This guard keeps the scan from
	// spinning over the whole map when those entries keep being skipped.
	skipped := 0
	maxSkipped := le.leaseExpiredNotifier.Len()

	for {
		l, next := le.expireExists()
		if l == nil && !next {
			break
		}
		if next {
			continue
		}

		if l.expired() {
			leases = append(leases, l)

			// reach expired limit
			if len(leases) >= limit {
				break
			}
			continue
		}
		skipped++
		if skipped >= maxSkipped {
			break
		}
	}

	return leases
}

func (le *lessor) scheduleCheckpointIfNeeded(lease *Lease) {
	if le.cp == nil {
		return
	}

	if lease.getRemainingTTL() > int64(le.checkpointInterval.Seconds()) {
		if le.lg != nil {
			le.lg.Debug("Scheduling lease checkpoint",
				zap.Int64("leaseID", int64(lease.ID)),
				zap.Duration("intervalSeconds", le.checkpointInterval),
			)
		}
		heap.Push(&le.leaseCheckpointHeap, &LeaseWithTime{
			id:   lease.ID,
			time: le.now().Add(le.checkpointInterval),
		})
	}
}

func (le *lessor) findDueScheduledCheckpoints(checkpointLimit int) []*pb.LeaseCheckpoint {
	if le.cp == nil {
		return nil
	}

	now := le.now()
	var cps []*pb.LeaseCheckpoint
	for le.leaseCheckpointHeap.Len() > 0 && len(cps) < checkpointLimit {
		lt := le.leaseCheckpointHeap[0]
		if lt.time.After(now) /* lt.time: next checkpoint time */ {
			return cps
		}
		heap.Pop(&le.leaseCheckpointHeap)
		var l *Lease
		var ok bool
		if l, ok = le.leaseMap[lt.id]; !ok {
			continue
		}
		if !now.Before(l.expiry) {
			continue
		}
		remainingTTL := int64(math.Ceil(l.expiry.Sub(now).Seconds()))
		if remainingTTL >= l.ttl {
			continue
		}
		if le.lg != nil {
			le.lg.Debug("Checkpointing lease",
				zap.Int64("leaseID", int64(lt.id)),
				zap.Int64("remainingTTL", remainingTTL),
			)
		}
		cps = append(cps, &pb.LeaseCheckpoint{ID: int64(lt.id), Remaining_TTL: remainingTTL})
	}
	return cps
}

func (le *lessor) initAndRecover() {
	tx := le.b.BatchTx()

	tx.LockOutsideApply()
	schema.UnsafeCreateLeaseBucket(tx)
	lpbs := schema.MustUnsafeGetAllLeases(tx)
	tx.Unlock()
	for _, lpb := range lpbs {
		ID := LeaseID(lpb.ID)
		if lpb.TTL < le.minLeaseTTL {
			lpb.TTL = le.minLeaseTTL
		}
		le.leaseMap[ID] = &Lease{
			ID:  ID,
			ttl: lpb.TTL,
			// itemSet will be filled in when recover key-value pairs
			// set expiry to forever, refresh when promoted
			itemSet:      make(map[LeaseItem]struct{}),
			expiry:       forever,
			revokec:      make(chan struct{}),
			remainingTTL: lpb.RemainingTTL,
			clock:        le.clock,
		}
	}
	// Leases come back from the backend as forever, so they cannot be
	// revoked or renewed until Promote refreshes their expiries. Start from
	// a clean revoked set for this incarnation of the lessor.
	le.recentlyRevoked = newRevokedLeases(revokedLeaseMemorySize)
	le.leaseExpiredNotifier.Init()
	heap.Init(&le.leaseCheckpointHeap)

	le.b.ForceCommit()
}

// FakeLessor is a fake implementation of Lessor interface.
// Used for testing only.
type FakeLessor struct {
	LeaseSet map[LeaseID]struct{}
}

func (fl *FakeLessor) SetRangeDeleter(dr RangeDeleter) {}

func (fl *FakeLessor) SetCheckpointer(cp Checkpointer) {}

func (fl *FakeLessor) Grant(id LeaseID, ttl int64) (*Lease, error) {
	fl.LeaseSet[id] = struct{}{}
	return nil, nil
}

func (fl *FakeLessor) Revoke(id LeaseID) error { return nil }

func (fl *FakeLessor) Checkpoint(id LeaseID, remainingTTL int64) error { return nil }

func (fl *FakeLessor) Attach(id LeaseID, items []LeaseItem) error { return nil }

func (fl *FakeLessor) GetLease(item LeaseItem) LeaseID            { return 0 }
func (fl *FakeLessor) Detach(id LeaseID, items []LeaseItem) error { return nil }

func (fl *FakeLessor) Promote(extend time.Duration) {}

func (fl *FakeLessor) Demote() {}

func (fl *FakeLessor) Renew(id LeaseID) (int64, error) { return 10, nil }

func (fl *FakeLessor) Lookup(id LeaseID) *Lease {
	if _, ok := fl.LeaseSet[id]; ok {
		return &Lease{ID: id}
	}
	return nil
}

func (fl *FakeLessor) Leases() []*Lease { return nil }

func (fl *FakeLessor) LeaseStats() LeaseStats { return LeaseStats{} }

func (fl *FakeLessor) ExpiredLeasesC() <-chan []*Lease { return nil }

func (fl *FakeLessor) Recover(b backend.Backend, rd RangeDeleter) {}

func (fl *FakeLessor) Stop() {}

type FakeTxnDelete struct {
	backend.BatchTx
}

func (ftd *FakeTxnDelete) DeleteRange(key, end []byte) (n, rev int64) { return 0, 0 }
func (ftd *FakeTxnDelete) End()                                       { ftd.Unlock() }
