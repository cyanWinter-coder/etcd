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

// Package watchbus implements a bounded-memory, revision-ordered
// in-process pub/sub bus for key events.
//
// A Bus runs a single dispatcher goroutine that serializes all routing
// decisions, which gives every watcher a strict revision order and keeps
// the hot path lock-free with respect to consumers. Each watcher owns a
// hard-capped event queue drained by its own pump goroutine: a stalled
// consumer can never block Publish or any other watcher, and once its
// cap is reached the watcher is cancelled with ErrBufferOverflow rather
// than accumulating an unbounded backlog.
//
// Typical use:
//
//	bus := watchbus.New(watchbus.Config{Logger: lg})
//	defer bus.Close()
//
//	w, err := bus.Watch(ctx, watchbus.WatchOptions{
//	    Key:         []byte("/foo"),
//	    BufferLimit: 256,
//	    Coalesce:    true, // fold consecutive same-key PUTs
//	})
//	if errors.Is(err, watchbus.ErrCompacted) {
//	    // start revision already compacted; re-establish from a snapshot
//	}
//	for resp := range w.Chan() {
//	    if resp.CancelReason != nil {
//	        // terminal: inspect the sentinel reason and CompactRevision
//	        break
//	    }
//	    // resp.Events are one revision's batch, in revision order
//	}
//
// The Stream type wraps one or more watchers into a single shared output
// channel with the same method shape as mvcc.WatchStream, for callers
// migrating from the existing watchable store without changing call
// sites.
package watchbus
