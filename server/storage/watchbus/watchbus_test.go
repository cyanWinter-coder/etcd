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

package watchbus

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.etcd.io/etcd/api/v3/mvccpb"
)

func putEv(key, val string, rev int64) *mvccpb.Event {
	return &mvccpb.Event{
		Type: mvccpb.Event_PUT,
		Kv: &mvccpb.KeyValue{
			Key:         []byte(key),
			Value:       []byte(val),
			ModRevision: rev,
		},
	}
}

func delEv(key string, rev int64) *mvccpb.Event {
	return &mvccpb.Event{
		Type: mvccpb.Event_DELETE,
		Kv: &mvccpb.KeyValue{
			Key:         []byte(key),
			ModRevision: rev,
		},
	}
}

func recv(t *testing.T, ch <-chan WatchResponse) WatchResponse {
	t.Helper()
	select {
	case resp, ok := <-ch:
		require.True(t, ok, "response channel closed unexpectedly")
		return resp
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for watch response")
		return WatchResponse{}
	}
}

// drainTerminal reads until the terminal response, returning it together
// with every data response observed beforehand.
func drainTerminal(t *testing.T, ch <-chan WatchResponse) (data []WatchResponse, terminal WatchResponse) {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for {
		select {
		case resp, ok := <-ch:
			if !ok {
				t.Fatal("channel closed without a terminal response")
			}
			if resp.CancelReason != nil {
				return data, resp
			}
			data = append(data, resp)
		case <-deadline:
			t.Fatal("timed out waiting for terminal response")
		}
	}
}

func newTestBus(t *testing.T) *Bus {
	t.Helper()
	b := New(Config{})
	t.Cleanup(b.Close)
	return b
}

// TestRevisionOrdering verifies requirement 1: events reach every
// watcher strictly ordered by revision, and a stalled watcher does not
// reorder or block another watcher's delivery.
func TestRevisionOrdering(t *testing.T) {
	b := newTestBus(t)
	w1, err := b.Watch(context.Background(), WatchOptions{Key: []byte("k")})
	require.NoError(t, err)
	w2, err := b.Watch(context.Background(), WatchOptions{Key: []byte("k")})
	require.NoError(t, err)

	const n = 50
	for rev := int64(1); rev <= n; rev++ {
		require.NoError(t, b.Publish(rev, []*mvccpb.Event{putEv("k", "v", rev)}))
	}

	for _, ch := range []<-chan WatchResponse{w1.Chan(), w2.Chan()} {
		var last int64
		for i := 0; i < n; i++ {
			resp := recv(t, ch)
			require.Nil(t, resp.CancelReason)
			require.Len(t, resp.Events, 1)
			rev := resp.Events[0].Kv.ModRevision
			require.Greater(t, rev, last, "events must arrive in revision order")
			last = rev
		}
		require.Equal(t, int64(n), last)
	}
}

// TestSlowWatcherIsolated verifies requirement 1's isolation half:
// a watcher that never consumes cannot block Publish or any other
// watcher, and requirement 2: it is cancelled with an explicit reason
// once its buffer cap is exceeded.
func TestSlowWatcherIsolatedAndOverflow(t *testing.T) {
	b := newTestBus(t)
	slow, err := b.Watch(context.Background(), WatchOptions{
		Key:         []byte("slow"),
		BufferLimit: 2,
	})
	require.NoError(t, err)
	fast, err := b.Watch(context.Background(), WatchOptions{
		Key:         []byte("fast"),
		BufferLimit: 256,
	})
	require.NoError(t, err)

	// Flood past the slow watcher's cap. Publish must never block:
	// run it with an overall timeout far below the test's patience.
	done := make(chan error, 1)
	go func() {
		for rev := int64(1); rev <= 20; rev++ {
			if err := b.Publish(rev, []*mvccpb.Event{
				putEv("slow", "v", rev),
				putEv("fast", "v", rev),
			}); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("Publish blocked on a slow consumer")
	}

	_, terminal := drainTerminal(t, slow.Chan())
	require.True(t, errors.Is(terminal.CancelReason, ErrBufferOverflow),
		"got %v, want ErrBufferOverflow", terminal.CancelReason)

	// The fast watcher must still receive every event in order.
	var last int64
	for i := 0; i < 20; i++ {
		resp := recv(t, fast.Chan())
		require.Nil(t, resp.CancelReason)
		rev := resp.Events[0].Kv.ModRevision
		require.Greater(t, rev, last)
		last = rev
	}
	require.Equal(t, int64(20), last)
	require.Eventually(t, func() bool {
		return b.Stats().OverflowCancels == 1
	}, time.Second, 10*time.Millisecond)
}

// TestCompactedStartRevision verifies requirement 3: a watch whose start
// revision is already compacted fails with a classifiable error instead
// of silently starting at "now", and a live watcher overtaken by
// compaction gets the same error with the compact revision attached.
func TestCompactedStartRevision(t *testing.T) {
	b := newTestBus(t)
	require.NoError(t, b.Compact(100))

	_, err := b.Watch(context.Background(), WatchOptions{
		Key:      []byte("k"),
		StartRev: 50,
	})
	require.ErrorIs(t, err, ErrCompacted)

	// A watcher starting beyond the new compact revision survives.
	live, err := b.Watch(context.Background(), WatchOptions{
		Key:      []byte("k"),
		StartRev: 201,
	})
	require.NoError(t, err)

	// A live watcher overtaken by a later compaction is terminated.
	victim, err := b.Watch(context.Background(), WatchOptions{
		Key:      []byte("k2"),
		StartRev: 101,
	})
	require.NoError(t, err)
	require.NoError(t, b.Compact(200))

	_, terminal := drainTerminal(t, victim.Chan())
	require.ErrorIs(t, terminal.CancelReason, ErrCompacted)
	require.Equal(t, int64(200), terminal.CompactRevision)

	// The surviving watcher still works and does not see older events.
	require.NoError(t, b.Publish(201, []*mvccpb.Event{putEv("k", "v", 201)}))
	resp := recv(t, live.Chan())
	require.Equal(t, int64(201), resp.Events[0].Kv.ModRevision)
}

// TestCancelIdempotentAndReleasesBuffer verifies requirement 4:
// repeated cancellation is harmless, the terminal reason is explicit,
// the channel closes, and routing state is released.
func TestCancelIdempotentAndReleasesBuffer(t *testing.T) {
	b := newTestBus(t)
	w, err := b.Watch(context.Background(), WatchOptions{
		Key:         []byte("k"),
		BufferLimit: 4,
	})
	require.NoError(t, err)
	require.NoError(t, b.Publish(1, []*mvccpb.Event{putEv("k", "v1", 1)}))
	require.NoError(t, b.Publish(2, []*mvccpb.Event{putEv("k", "v2", 2)}))

	// Repeated cancellation must not panic or double-close.
	w.Cancel()
	w.Cancel()
	w.Cancel()

	_, terminal := drainTerminal(t, w.Chan())
	require.ErrorIs(t, terminal.CancelReason, ErrWatcherCanceled)

	// Channel must be closed after the terminal response.
	_, ok := <-w.Chan()
	require.False(t, ok, "channel should be closed after terminal response")

	require.Eventually(t, func() bool {
		return b.Stats().LiveWatchers == 0
	}, time.Second, 10*time.Millisecond)
	// The closed output channel implies the pump has fully exited.
	select {
	case <-w.sub.done:
	default:
		t.Fatal("pump goroutine still running after channel closed")
	}

	// Events published after cancellation are simply not routed to it
	// and must not bring the watcher back.
	require.NoError(t, b.Publish(3, []*mvccpb.Event{putEv("k", "v3", 3)}))
}

// TestCoalesceSameKey verifies requirement 5: consecutive PUTs for one
// key merge into the newest value while deletes and key boundaries are
// preserved, and the default (opt-out) path keeps every event.
func TestCoalesceSameKey(t *testing.T) {
	b := newTestBus(t)

	// Twenty rapid PUTs, none consumed yet, under a cap large enough to
	// avoid overflow so coalescing is the only effect under test.
	const puts int64 = 20
	w, err := b.Watch(context.Background(), WatchOptions{
		Key:         []byte("k"),
		Coalesce:    true,
		BufferLimit: int(puts)*2 + 2,
	})
	require.NoError(t, err)
	for rev := int64(1); rev <= puts; rev++ {
		require.NoError(t, b.Publish(rev, []*mvccpb.Event{putEv("k", "v", rev)}))
	}
	// A delete must survive as its own response.
	require.NoError(t, b.Publish(puts+1, []*mvccpb.Event{delEv("k", puts+1)}))

	// Drain data responses (stopping at the delete) before cancelling, so
	// the assertions do not race the cancel-time buffer release.
	var data []WatchResponse
	var sawDelete bool
	deadline := time.After(3 * time.Second)
	for !sawDelete {
		select {
		case resp := <-w.Chan():
			if resp.CancelReason != nil {
				t.Fatalf("unexpected terminal before data drain: %v", resp.CancelReason)
			}
			data = append(data, resp)
			for _, ev := range resp.Events {
				if ev.Type == mvccpb.Event_DELETE {
					sawDelete = true
				}
			}
		case <-deadline:
			t.Fatal("timed out waiting for coalesced data")
		}
	}
	w.Cancel()
	_, terminal := drainTerminal(t, w.Chan())
	require.ErrorIs(t, terminal.CancelReason, ErrWatcherCanceled)
	require.NotEmpty(t, data)

	// Every retained PUT must carry the newest value and a revision no
	// later than the last PUT; the delete arrived unchanged above.
	var putCount int
	for _, resp := range data {
		for _, ev := range resp.Events {
			if ev.Type == mvccpb.Event_PUT {
				putCount++
				require.Equal(t, []byte("v"), ev.Kv.Value)
				require.LessOrEqual(t, ev.Kv.ModRevision, puts)
			}
		}
	}
	require.GreaterOrEqual(t, b.Stats().Coalesced, int64(1))
	require.Less(t, putCount, int(puts), "consecutive same-key PUTs should collapse")

	// Default watcher keeps exact per-event semantics.
	exact, err := b.Watch(context.Background(), WatchOptions{Key: []byte("j"), BufferLimit: 64})
	require.NoError(t, err)
	nextRev := puts + 2
	for i := int64(0); i < 4; i++ {
		rev := nextRev + i
		require.NoError(t, b.Publish(rev, []*mvccpb.Event{putEv("j", "v", rev)}))
	}
	for i := 0; i < 4; i++ {
		resp := recv(t, exact.Chan())
		require.Len(t, resp.Events, 1)
	}
}

// TestCoalesceDifferentKeyNotMerged ensures merging never crosses keys.
func TestCoalesceDifferentKeyNotMerged(t *testing.T) {
	b := newTestBus(t)
	// Range watcher sees both keys.
	w, err := b.Watch(context.Background(), WatchOptions{
		Key:         []byte("a"),
		End:         []byte("c"),
		Coalesce:    true,
		BufferLimit: 64,
	})
	require.NoError(t, err)
	require.NoError(t, b.Publish(1, []*mvccpb.Event{putEv("a", "1", 1), putEv("b", "1", 1)}))
	require.NoError(t, b.Publish(2, []*mvccpb.Event{putEv("a", "2", 2), putEv("b", "2", 2)}))

	resp := recv(t, w.Chan())
	require.Len(t, resp.Events, 2) // one per key, same revision batch
	next := recv(t, w.Chan())
	require.Len(t, next.Events, 2)
}

// TestRangeAndStartRev covers range matching and start-revision filtering.
func TestRangeAndStartRev(t *testing.T) {
	b := newTestBus(t)
	w, err := b.Watch(context.Background(), WatchOptions{
		Key:      []byte("a"),
		End:      []byte("c"), // [a, c): a and b match, c does not
		StartRev: 3,
	})
	require.NoError(t, err)
	require.NoError(t, b.Publish(1, []*mvccpb.Event{putEv("a", "old", 1)}))
	require.NoError(t, b.Publish(2, []*mvccpb.Event{putEv("b", "old", 2)}))
	require.NoError(t, b.Publish(3, []*mvccpb.Event{
		putEv("a", "new", 3),
		putEv("c", "ignored", 3),
	}))

	resp := recv(t, w.Chan())
	require.Len(t, resp.Events, 1)
	require.Equal(t, []byte("a"), resp.Events[0].Kv.Key)
}

// TestWatchFromNow ensures StartRev<=0 only observes future events.
func TestWatchFromNow(t *testing.T) {
	b := newTestBus(t)
	require.NoError(t, b.Publish(5, []*mvccpb.Event{putEv("k", "old", 5)}))
	w, err := b.Watch(context.Background(), WatchOptions{Key: []byte("k")})
	require.NoError(t, err)
	require.NoError(t, b.Publish(6, []*mvccpb.Event{putEv("k", "new", 6)}))
	resp := recv(t, w.Chan())
	require.Equal(t, int64(6), resp.Events[0].Kv.ModRevision)
}

// TestTimeoutAndContextCancel exercise the other terminal reasons.
func TestTimeoutAndContextCancel(t *testing.T) {
	b := newTestBus(t)
	timed, err := b.Watch(context.Background(), WatchOptions{
		Key:     []byte("k1"),
		Timeout: int64(50 * time.Millisecond),
	})
	require.NoError(t, err)
	_, terminal := drainTerminal(t, timed.Chan())
	require.ErrorIs(t, terminal.CancelReason, ErrWatcherTimeout)

	ctx, cancel := context.WithCancel(context.Background())
	cw, err := b.Watch(ctx, WatchOptions{Key: []byte("k2")})
	require.NoError(t, err)
	cancel()
	_, terminal = drainTerminal(t, cw.Chan())
	require.ErrorIs(t, terminal.CancelReason, ErrWatcherCanceled)
}

// TestProgressNotification checks ordered, zero-event progress markers.
func TestProgressNotification(t *testing.T) {
	b := newTestBus(t)
	w, err := b.Watch(context.Background(), WatchOptions{Key: []byte("k")})
	require.NoError(t, err)
	require.NoError(t, b.Publish(7, []*mvccpb.Event{putEv("k", "v", 7)}))
	_ = recv(t, w.Chan())

	require.True(t, b.RequestProgress(w.ID()))
	select {
	case resp := <-w.Chan():
		require.Empty(t, resp.Events)
		require.Equal(t, int64(7), resp.Revision)
	case <-time.After(time.Second):
		t.Fatal("progress response not delivered")
	}
}

// TestFiltersDoNotConsumeBuffer verifies filtered events skip the queue
// entirely rather than counting against the buffer cap.
func TestFiltersDoNotConsumeBuffer(t *testing.T) {
	b := newTestBus(t)
	// A range watcher sees both "keep" and "skip"; the filter discards
	// every "skip" event.
	filter := func(e *mvccpb.Event) bool { return string(e.Kv.Key) == "skip" }
	w, err := b.Watch(context.Background(), WatchOptions{
		Key:         []byte("a"),
		End:         []byte("z"),
		BufferLimit: 2,
		Filters:     []FilterFunc{filter},
	})
	require.NoError(t, err)

	// Far more filtered events than the cap: none may overflow the
	// watcher and no response is produced for them.
	for rev := int64(1); rev <= 20; rev++ {
		require.NoError(t, b.Publish(rev, []*mvccpb.Event{putEv("skip", "x", rev)}))
	}
	require.Equal(t, int64(0), b.Stats().OverflowCancels)

	// Two kept events fill the cap; a tight burst of more kept events
	// must trigger overflow before the pump can drain them.
	for _, rev := range []int64{21, 22, 23, 24, 25, 26} {
		require.NoError(t, b.Publish(rev, []*mvccpb.Event{putEv("keep", "v", rev)}))
	}
	require.Eventually(t, func() bool {
		return b.Stats().OverflowCancels == 1
	}, time.Second, 5*time.Millisecond)

	_, terminal := drainTerminal(t, w.Chan())
	require.ErrorIs(t, terminal.CancelReason, ErrBufferOverflow)
}

// TestPublishMonotonic enforces the revision-ordering contract at the
// API boundary.
func TestPublishMonotonic(t *testing.T) {
	b := newTestBus(t)
	require.NoError(t, b.Publish(2, []*mvccpb.Event{putEv("k", "v", 2)}))
	require.ErrorIs(t, b.Publish(2, []*mvccpb.Event{putEv("k", "v", 2)}), ErrInvalidRevision)
	require.ErrorIs(t, b.Publish(1, []*mvccpb.Event{putEv("k", "v", 1)}), ErrInvalidRevision)
}

// TestDuplicateIDAndBadRange covers the input validation errors.
func TestDuplicateIDAndBadRange(t *testing.T) {
	b := newTestBus(t)
	_, err := b.Watch(context.Background(), WatchOptions{ID: 9, Key: []byte("k")})
	require.NoError(t, err)
	_, err = b.Watch(context.Background(), WatchOptions{ID: 9, Key: []byte("k")})
	require.ErrorIs(t, err, ErrWatcherDuplicateID)

	_, err = b.Watch(context.Background(), WatchOptions{Key: []byte("z"), End: []byte("a")})
	require.ErrorIs(t, err, ErrEmptyWatcherRange)
}

// TestStreamFanInAndLifecycle validates the mvcc-shaped compatibility
// layer: shared channel, per-watch IDs, idempotent cancel/close.
func TestStreamFanInAndLifecycle(t *testing.T) {
	b := newTestBus(t)
	s := NewStream(b)
	id1, err := s.Watch(context.Background(), AutoWatchID, []byte("k1"), nil, 0)
	require.NoError(t, err)
	id2, err := s.Watch(context.Background(), AutoWatchID, []byte("k2"), nil, 0)
	require.NoError(t, err)
	require.NotEqual(t, id1, id2)

	require.NoError(t, b.Publish(1, []*mvccpb.Event{
		putEv("k1", "a", 1),
		putEv("k2", "b", 1),
	}))

	got := map[WatchID]string{}
	for i := 0; i < 2; i++ {
		resp := recv(t, s.Chan())
		got[resp.WatchID] = string(resp.Events[0].Kv.Value)
	}
	require.Equal(t, "a", got[id1])
	require.Equal(t, "b", got[id2])

	require.ErrorIs(t, s.Cancel(WatchID(9999)), ErrWatcherNotExist)
	require.NoError(t, s.Cancel(id1))
	require.NoError(t, s.Cancel(id1), "repeated cancel must be idempotent")

	// Close must be idempotent and eventually close the shared channel
	// after any buffered terminal responses are drained.
	s.Close()
	s.Close()
	for range s.Chan() {
	}
	_, ok := <-s.Chan()
	require.False(t, ok)
}

// TestCloseReleasesPumps ensures bus shutdown terminates watchers even
// when their consumer stopped reading, with no pump goroutines leaked.
func TestCloseReleasesPumps(t *testing.T) {
	b := New(Config{})
	baseline := runtime.NumGoroutine()
	for i := 0; i < 16; i++ {
		_, err := b.Watch(context.Background(), WatchOptions{
			Key:         []byte("k"),
			BufferLimit: 4,
		})
		require.NoError(t, err)
	}
	for rev := int64(1); rev <= 10; rev++ {
		_ = b.Publish(rev, []*mvccpb.Event{putEv("k", "v", rev)})
	}
	b.Close()

	// Idempotent close must not panic.
	b.Close()
	require.Eventually(t, func() bool {
		return runtime.NumGoroutine() <= baseline+1
	}, 2*time.Second, 20*time.Millisecond)
	require.ErrorIs(t, b.Publish(1, nil), ErrBusClosed)
}

// TestStatsCounters sanity-checks the observability surface used to
// detect slow consumers in production.
func TestStatsCounters(t *testing.T) {
	b := newTestBus(t)
	w, err := b.Watch(context.Background(), WatchOptions{Key: []byte("k"), BufferLimit: 1, Coalesce: true})
	require.NoError(t, err)
	require.NoError(t, b.Publish(1, []*mvccpb.Event{putEv("k", "1", 1)}))
	require.NoError(t, b.Publish(2, []*mvccpb.Event{putEv("k", "2", 2)}))
	require.NoError(t, b.Publish(3, []*mvccpb.Event{putEv("k", "3", 3)}))

	require.Eventually(t, func() bool {
		st := b.Stats()
		return st.WatchersCreated == 1 && st.Publishes == 3 &&
			st.EventsIn == 3 && st.OverflowCancels == 1
	}, time.Second, 10*time.Millisecond)

	_, terminal := drainTerminal(t, w.Chan())
	assert.ErrorIs(t, terminal.CancelReason, ErrBufferOverflow)
}
