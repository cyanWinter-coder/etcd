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
	"sync"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
)

// queuedItem is one element of a watcher's output pipeline. Only the
// watcher's pump goroutine ever sends on out, so data, progress and the
// terminal message always leave in FIFO order.
type queuedItem struct {
	resp     WatchResponse
	progress bool
	terminal bool
}

// subscription is the internal state of one watcher. A *subscription is
// owned by exactly one goroutine (the bus dispatcher) for routing and
// bookkeeping; enqueue/terminate additionally take w.mu so the watcher
// handle and its pump goroutine can drive cancellation concurrently.
type subscription struct {
	id       WatchID
	key      []byte
	end      []byte // nil means a single-key watcher
	minRev   int64  // first revision this watcher still expects
	limit    int
	coalesce bool
	filters  []FilterFunc

	// out is the consumer-facing channel. The pump is its only sender
	// and its only closer.
	out chan WatchResponse

	// mu guards pending, inOut, terminated and terminal.
	mu         sync.Mutex
	pending    []queuedItem
	inOut      bool // one item may be parked in out
	wake       chan struct{}
	terminated bool
	terminal   queuedItem

	// done is closed when the pump goroutine exits, so timeout/cancel
	// helpers and bus shutdown can wait for full cleanup.
	done chan struct{}
}

// Watcher is the public handle returned by Bus.Watch. Reading Chan()
// until it closes drains every guaranteed response, including the
// terminal one explaining why the watcher ended.
type Watcher struct {
	sub *subscription
	bus *Bus
}

// Chan returns the channel on which event, progress and terminal
// responses are delivered, in order.
func (w *Watcher) Chan() <-chan WatchResponse { return w.sub.out }

// ID returns the watcher ID (resolved if AutoWatchID was requested).
func (w *Watcher) ID() WatchID { return w.sub.id }

// Cancel terminates the watcher with ErrWatcherCanceled. It is safe to
// call concurrently and any number of times: only the first termination
// wins, later calls are no-ops.
func (w *Watcher) Cancel() {
	w.sub.terminate(WatchResponse{
		WatchID:      w.sub.id,
		CancelReason: ErrWatcherCanceled,
	})
}

// RequestProgress enqueues a best-effort zero-event progress response.
func (w *Watcher) RequestProgress() {
	w.bus.RequestProgress(w.sub.id)
}

// runPump is the watcher's sole sender goroutine. It moves queued items
// onto the output channel one at a time and guarantees the channel is
// closed exactly once after the terminal response has been delivered (or
// the bus is shutting down).
func (w *subscription) runPump(bus *Bus) {
	defer close(w.done)
	defer close(w.out)

	for {
		w.mu.Lock()
		for len(w.pending) == 0 && !w.terminated {
			w.mu.Unlock()
			select {
			case <-w.wake:
			case <-bus.stopc:
				// Bus shutting down; loop and re-check state.
			}
			w.mu.Lock()
		}

		if len(w.pending) > 0 {
			item := w.pending[0]
			w.pending = w.pending[1:]
			w.inOut = true
			w.mu.Unlock()

			select {
			case w.out <- item.resp:
			case <-bus.stopc:
				// No consumer may read this anymore; wind down.
				w.mu.Lock()
				w.inOut = false
				w.pending = nil
				if !w.terminated {
					w.terminated = true
					w.terminal = queuedItem{terminal: true, resp: WatchResponse{
						WatchID:      w.id,
						CancelReason: ErrBusClosed,
					}}
				}
				w.mu.Unlock()
				return
			}

			w.mu.Lock()
			w.inOut = false
			terminated := w.terminated
			more := len(w.pending) > 0
			w.mu.Unlock()
			if !more && terminated {
				// terminate() already dropped the queue and staged the
				// terminal message; emit it as the final response.
				w.emitTerminal(bus)
				return
			}
			continue
		}

		// No pending data and terminated: emit the final response.
		w.pending = nil // release anything a racing terminator dropped
		w.mu.Unlock()
		w.emitTerminal(bus)
		return
	}
}

// emitTerminal sends the already-decided terminal response. It runs
// exactly once, immediately before runPump returns and closes out.
func (w *subscription) emitTerminal(bus *Bus) {
	w.mu.Lock()
	terminal := w.terminal
	w.mu.Unlock()
	select {
	case w.out <- terminal.resp:
	case <-bus.stopc:
	}
}

// queuedEvents reports how many event items are currently buffered:
// in-memory queue plus the item parked in the output channel. Progress
// markers do not count against the event buffer cap.
func (w *subscription) queuedEvents() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	n := len(w.pending)
	if w.inOut {
		n++
	}
	return n
}

// enqueueBatch adds all events of one revision (already matched and
// filtered for this watcher by the dispatcher) as a single response
// item, matching mvcc's one-response-per-revision delivery. The returned
// accepted is false when the hard buffer cap is already reached, so the
// dispatcher can terminate the watcher with ErrBufferOverflow. When
// accepted and coalesced is true, a single-event PUT was folded into
// the previous queued PUT for the same key and consumed no new buffer
// slot. The caller is the single dispatcher goroutine; w.mu serializes
// against Cancel/timeout running on other goroutines.
func (w *subscription) enqueueBatch(evs []*mvccpb.Event, rev int64) (accepted, coalesced bool) {
	if len(evs) == 0 {
		return true, false
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.terminated {
		return false, false
	}

	eventCount := len(w.pending)
	if w.inOut {
		eventCount++
	}
	if eventCount >= w.limit {
		return false, false
	}

	// Coalescing only applies to a lone PUT following another lone PUT
	// on the same key; multi-event revisions and delete boundaries are
	// always delivered intact.
	if w.coalesce && len(evs) == 1 && w.tryCoalesceLocked(evs[0]) {
		w.signalLocked()
		return true, true
	}

	// Each watcher gets its own slice; the dispatcher's batch slice is
	// shared across watchers and must not be retained by reference.
	owned := make([]*mvccpb.Event, len(evs))
	copy(owned, evs)
	w.pending = append(w.pending, queuedItem{
		resp: WatchResponse{WatchID: w.id, Events: owned, Revision: rev},
	})
	w.signalLocked()
	return true, false
}

// applyFilters drops filtered events from a matched batch before any
// buffer capacity is consumed.
func (w *subscription) applyFilters(evs []*mvccpb.Event) []*mvccpb.Event {
	if len(w.filters) == 0 {
		return evs
	}
	var kept []*mvccpb.Event
	for _, ev := range evs {
		filtered := false
		for _, f := range w.filters {
			if f(ev) {
				filtered = true
				break
			}
		}
		if !filtered {
			kept = append(kept, ev)
		}
	}
	return kept
}

// tryCoalesceLocked merges ev into the last queued event when both are
// consecutive PUTs on the same key. A PUT following a PUT for the same
// key carries no intermediate observable state (the consumer could not
// have read the older value from this channel yet), so keeping only the
// newest PUT preserves every state it can observe while shrinking the
// buffer. Deletes, different keys and PUT-after-DELETE boundaries are
// never merged. Caller holds w.mu.
func (w *subscription) tryCoalesceLocked(ev *mvccpb.Event) bool {
	if ev.Type != mvccpb.Event_PUT || len(w.pending) == 0 {
		return false
	}
	last := &w.pending[len(w.pending)-1]
	if last.terminal || last.progress || len(last.resp.Events) != 1 {
		return false
	}
	prev := last.resp.Events[0]
	if prev.Type != mvccpb.Event_PUT || !bytesEqual(prev.Kv.Key, ev.Kv.Key) {
		return false
	}
	// Replace with the newer PUT and advance the response revision.
	last.resp.Events[0] = ev
	last.resp.Revision = ev.Kv.ModRevision
	return true
}

// requestProgress enqueues a zero-event progress marker, best effort.
// It never blocks the dispatcher: if the watcher's queue is saturated the
// request is simply dropped (a later progress request or event will
// confirm the same revision).
func (w *subscription) requestProgress(rev int64) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.terminated {
		return false
	}
	w.pending = append(w.pending, queuedItem{
		progress: true,
		resp:     WatchResponse{WatchID: w.id, Revision: rev},
	})
	w.signalLocked()
	return true
}

// terminate marks the watcher as finished with a terminal response. It
// is idempotent: the first caller decides the reason, subsequent calls
// are ignored. The pending event buffer is released immediately.
func (w *subscription) terminate(resp WatchResponse) {
	w.mu.Lock()
	if w.terminated {
		w.mu.Unlock()
		return
	}
	w.terminated = true
	w.terminal = queuedItem{terminal: true, resp: resp}
	// Release buffered events eagerly; the pump only needs the terminal.
	w.pending = nil
	w.signalLocked()
	w.mu.Unlock()
}

// signalLocked wakes the pump if it is parked. Non-blocking by design:
// wake has capacity one and a token already present implies the pump
// will observe the new state when it next loops.
func (w *subscription) signalLocked() {
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// watchCancelOnContext terminates the watcher when ctx ends before the
// pump exits. The goroutine always exits when the pump finishes, so no
// goroutine leaks across the watcher lifecycle.
func (w *subscription) watchCancelOnContext(ctx context.Context) {
	if ctx == context.Background() || ctx == context.TODO() {
		return
	}
	go func() {
		select {
		case <-ctx.Done():
			reason := ErrWatcherCanceled
			if ctx.Err() == context.DeadlineExceeded {
				reason = ErrWatcherTimeout
			}
			w.terminate(WatchResponse{WatchID: w.id, CancelReason: reason})
		case <-w.done:
		}
	}()
}

// watchTimeout enforces the optional hard lifetime.
func (w *subscription) watchTimeout(timeout time.Duration) {
	if timeout <= 0 {
		return
	}
	t := time.AfterFunc(timeout, func() {
		w.terminate(WatchResponse{WatchID: w.id, CancelReason: ErrWatcherTimeout})
	})
	go func() {
		<-w.done
		t.Stop()
	}()
}
