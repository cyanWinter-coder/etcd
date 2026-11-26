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

// Stats is a point-in-time snapshot of bus counters. All counters are
// monotonic for the lifetime of a Bus except LiveWatchers, which is a
// gauge. Every field is written only by the dispatcher goroutine and
// published through Stats(), so reading a snapshot is race-free.
type Stats struct {
	// LiveWatchers is the current number of routed watchers.
	LiveWatchers int
	// WatchersCreated counts watchers successfully created.
	WatchersCreated int64
	// WatchersCanceled counts watchers removed after their pumps exited.
	WatchersCanceled int64
	// Publishes counts processed Publish calls.
	Publishes int64
	// EventsIn counts events submitted to Publish.
	EventsIn int64
	// EventsDelivered counts events accepted into a watcher's queue.
	// A coalesced event is counted here as well as in Coalesced.
	EventsDelivered int64
	// OverflowCancels counts watchers terminated for hitting their
	// buffer cap.
	OverflowCancels int64
	// CompactCancels counts live watchers terminated by Compact.
	CompactCancels int64
	// CompactRejects counts Watch calls rejected for a compacted
	// start revision.
	CompactRejects int64
	// Coalesced counts how many events were folded into a previous
	// queued event instead of occupying a new buffer slot.
	Coalesced int64
}

// Stats returns a consistent snapshot of the bus counters. It is served
// by the dispatcher goroutine, so it never observes a half-updated
// counter set. After Close it returns the final snapshot directly.
func (b *Bus) Stats() Stats {
	if b.closed.Load() {
		return b.stats
	}
	resp := make(chan Stats, 1)
	select {
	case b.statsc <- resp:
	case <-b.stopc:
		return b.stats
	}
	select {
	case s := <-resp:
		return s
	case <-b.stopc:
		return b.stats
	}
}
