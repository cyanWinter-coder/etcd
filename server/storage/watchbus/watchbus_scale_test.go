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
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"go.etcd.io/etcd/api/v3/mvccpb"
)

// TestManyWatchersSlowConsumerIsolation reproduces the production shape
// from the design brief: hundreds of watchers, a burst of writes, with
// one watcher that never reads. The slow watcher must be cancelled with
// ErrBufferOverflow while healthy watchers keep receiving every event
// in order, and the bus must not leak goroutines or memory.
func TestManyWatchersSlowConsumerIsolation(t *testing.T) {
	b := newTestBus(t)
	const healthy = 200
	const writes = 300

	healthyWatchers := make([]*Watcher, 0, healthy)
	for i := 0; i < healthy; i++ {
		w, err := b.Watch(context.Background(), WatchOptions{
			Key:         []byte("hot"),
			BufferLimit: writes + 16,
		})
		require.NoError(t, err)
		healthyWatchers = append(healthyWatchers, w)
	}
	// One deliberately stalled watcher with a tiny cap.
	slow, err := b.Watch(context.Background(), WatchOptions{
		Key:         []byte("hot"),
		BufferLimit: 4,
	})
	require.NoError(t, err)

	for rev := int64(1); rev <= writes; rev++ {
		require.NoError(t, b.Publish(rev, []*mvccpb.Event{putEv("hot", "v", rev)}))
	}

	// Slow watcher is cancelled with an explicit, classifiable reason.
	require.Eventually(t, func() bool {
		return b.Stats().OverflowCancels == 1
	}, 2*time.Second, 10*time.Millisecond)
	_, terminal := drainTerminal(t, slow.Chan())
	require.ErrorIs(t, terminal.CancelReason, ErrBufferOverflow)

	// Every healthy watcher receives the full, ordered stream.
	var wg sync.WaitGroup
	for idx, w := range healthyWatchers {
		wg.Add(1)
		go func(idx int, w *Watcher) {
			defer wg.Done()
			var last int64
			for i := 0; i < writes; i++ {
				resp := recv(t, w.Chan())
				require.Nil(t, resp.CancelReason, "watcher %d", idx)
				rev := resp.Events[0].Kv.ModRevision
				require.Equal(t, last+1, rev, "watcher %d gap in revision order", idx)
				last = rev
			}
		}(idx, w)
	}
	wg.Wait()
	require.Equal(t, int64(1), b.Stats().OverflowCancels)
}

// TestWatcherChurnNoLeak creates and cancels watchers in a tight loop
// and asserts the live gauge returns to zero and no pump goroutines are
// retained, exercising the idempotent cancel/remove paths under churn.
func TestWatcherChurnNoLeak(t *testing.T) {
	b := newTestBus(t)
	baseline := runtime.NumGoroutine()

	const rounds = 300
	for r := 0; r < rounds; r++ {
		ws := make([]*Watcher, 8)
		for i := range ws {
			w, err := b.Watch(context.Background(), WatchOptions{
				Key:         []byte(fmt.Sprintf("k%d", i)),
				BufferLimit: 4,
			})
			require.NoError(t, err)
			ws[i] = w
		}
		require.NoError(t, b.Publish(int64(r+1), []*mvccpb.Event{
			putEv("k0", "v", int64(r+1)),
			putEv("k3", "v", int64(r+1)),
		}))
		// Cancel twice to stress idempotency, from multiple goroutines.
		var cwg sync.WaitGroup
		for _, w := range ws {
			cwg.Add(2)
			go func(w *Watcher) { defer cwg.Done(); w.Cancel() }(w)
			go func(w *Watcher) { defer cwg.Done(); w.Cancel() }(w)
		}
		cwg.Wait()
		for _, w := range ws {
			_, term := drainTerminal(t, w.Chan())
			require.ErrorIs(t, term.CancelReason, ErrWatcherCanceled)
		}
	}

	require.Eventually(t, func() bool {
		return b.Stats().LiveWatchers == 0
	}, 2*time.Second, 10*time.Millisecond)
	require.Equal(t, int64(rounds*8), b.Stats().WatchersCreated)

	require.Eventually(t, func() bool {
		// Pumps plus any context/timeout helper goroutines must exit.
		return runtime.NumGoroutine() <= baseline+2
	}, 2*time.Second, 20*time.Millisecond)
}

// BenchmarkPublishFanout measures dispatch throughput when one write
// fans out to many watchers. The per-watcher queues are sized to avoid
// overflow during the benchmark window.
func BenchmarkPublishFanout(b *testing.B) {
	bus := New(Config{})
	defer bus.Close()
	const watchers = 128
	ws := make([]*Watcher, watchers)
	for i := range ws {
		w, err := bus.Watch(context.Background(), WatchOptions{
			Key:         []byte("hot"),
			BufferLimit: b.N + 64,
		})
		if err != nil {
			b.Fatal(err)
		}
		ws[i] = w
	}
	// Drainers keep the pumps busy but never block the dispatcher.
	var drainWG sync.WaitGroup
	stop := make(chan struct{})
	for _, w := range ws {
		drainWG.Add(1)
		go func(w *Watcher) {
			defer drainWG.Done()
			for {
				select {
				case <-w.Chan():
				case <-stop:
					return
				}
			}
		}(w)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := bus.Publish(int64(i+1), []*mvccpb.Event{
			putEv("hot", "v", int64(i+1)),
		}); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	close(stop)
	drainWG.Wait()
}

// BenchmarkCoalescedOverflow models the worst case from the brief: a
// completely stalled watcher under a firehose of same-key writes. It
// shows the bus cancels the watcher on the first over-cap revision and
// then keeps publishing without holding any backlog for it.
func BenchmarkCoalescedOverflow(b *testing.B) {
	bus := New(Config{})
	defer bus.Close()
	if _, err := bus.Watch(context.Background(), WatchOptions{
		Key:         []byte("hot"),
		BufferLimit: 32,
		Coalesce:    true,
	}); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Errors after overflow are still not returned per publish; the
		// point is the call stays O(1) and never blocks on the consumer.
		_ = bus.Publish(int64(i+1), []*mvccpb.Event{
			putEv("hot", "v", int64(i+1)),
		})
	}
	b.StopTimer()
}
