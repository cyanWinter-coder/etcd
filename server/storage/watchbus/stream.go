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
)

// streamEntry pairs a watcher with its cancellation flag. The entry is
// retained after Cancel (rather than deleted) so repeated cancellation
// is idempotent, while a canceled ID cannot be reused by a new watcher
// until the stream is closed.
type streamEntry struct {
	w        *Watcher
	canceled bool
}

// Stream fans multiple watchers into one output channel, mirroring the
// method set of mvcc.WatchStream so existing callers can migrate without
// changing call sites. Advanced controls (coalescing, buffer cap,
// timeout) are opt-in via WatchWith; plain Watch keeps the historical
// per-event semantics and defaults.
type Stream struct {
	bus *Bus

	mu       sync.Mutex
	closed   bool
	stopc    chan struct{}
	watchers map[WatchID]*streamEntry
	forwards sync.WaitGroup

	// out is the shared consumer channel. Each watcher's own bounded
	// queue remains the backpressure boundary; forwarders only move
	// already-accepted responses here.
	out chan WatchResponse
}

// NewStream creates a fan-in stream on the bus. The shared output
// channel is buffered with DefaultBufferLimit; when it is full the
// affected watcher's forwarder blocks inside its own goroutine while
// every other watcher keeps dispatching.
func NewStream(bus *Bus) *Stream {
	return &Stream{
		bus:      bus,
		watchers: make(map[WatchID]*streamEntry),
		stopc:    make(chan struct{}),
		out:      make(chan WatchResponse, DefaultBufferLimit),
	}
}

// Chan returns the shared response channel. It is closed by Close.
func (s *Stream) Chan() <-chan WatchResponse { return s.out }

// Rev returns the bus's current revision.
func (s *Stream) Rev() int64 { return s.bus.Rev() }

// Watch creates a watcher with default options, matching the
// mvcc.WatchStream signature (key/end/startRev plus optional filters).
func (s *Stream) Watch(ctx context.Context, id WatchID, key, end []byte, startRev int64, fcs ...FilterFunc) (WatchID, error) {
	return s.WatchWith(ctx, WatchOptions{
		ID:       id,
		Key:      key,
		End:      end,
		StartRev: startRev,
		Filters:  fcs,
	})
}

// WatchWith creates a watcher with full WatchOptions (coalescing,
// explicit buffer cap, timeout). The default values of WatchOptions
// reproduce plain Watch behavior.
func (s *Stream) WatchWith(ctx context.Context, opts WatchOptions) (WatchID, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return InvalidWatchID, ErrBusClosed
	}
	if opts.ID != AutoWatchID {
		if _, exists := s.watchers[opts.ID]; exists {
			s.mu.Unlock()
			return InvalidWatchID, ErrWatcherDuplicateID
		}
	}
	s.mu.Unlock()

	w, err := s.bus.Watch(ctx, opts)
	if err != nil {
		return InvalidWatchID, err
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		w.Cancel()
		return InvalidWatchID, ErrBusClosed
	}
	if _, exists := s.watchers[w.ID()]; exists {
		s.mu.Unlock()
		w.Cancel()
		return InvalidWatchID, ErrWatcherDuplicateID
	}
	s.watchers[w.ID()] = &streamEntry{w: w}
	s.mu.Unlock()

	s.forwards.Add(1)
	go s.forward(w)
	return w.ID(), nil
}

// forward pumps one watcher's ordered responses into the shared stream
// channel until that watcher terminates and its channel is closed.
func (s *Stream) forward(w *Watcher) {
	defer s.forwards.Done()
	for resp := range w.Chan() {
		select {
		case s.out <- resp:
		case <-s.stopc:
			// Stream is shutting down; keep draining the watcher so its
			// pump can exit and release its buffer.
			continue
		}
	}
}

// Cancel cancels one watcher. Repeated cancellation of the same ID and
// cancellation of an already-terminated watcher are idempotent and
// return nil; an ID never registered on this stream returns
// ErrWatcherNotExist.
func (s *Stream) Cancel(id WatchID) error {
	s.mu.Lock()
	entry, ok := s.watchers[id]
	s.mu.Unlock()
	if !ok {
		return ErrWatcherNotExist
	}
	entry.w.Cancel() // idempotent at the subscription level
	entry.canceled = true
	return nil
}

// RequestProgress enqueues a progress notification for one active
// watcher. It is a no-op for canceled or unknown IDs.
func (s *Stream) RequestProgress(id WatchID) {
	s.mu.Lock()
	entry, ok := s.watchers[id]
	s.mu.Unlock()
	if ok && !entry.canceled {
		s.bus.RequestProgress(id)
	}
}

// RequestProgressAll enqueues a progress notification for every active
// watcher on the stream and returns true when at least one request was
// sent.
func (s *Stream) RequestProgressAll() bool {
	s.mu.Lock()
	pending := make([]WatchID, 0, len(s.watchers))
	for id, entry := range s.watchers {
		if !entry.canceled {
			pending = append(pending, id)
		}
	}
	s.mu.Unlock()
	for _, id := range pending {
		s.bus.RequestProgress(id)
	}
	return len(pending) > 0
}

// Close cancels every watcher, waits for all forwarders to finish, then
// closes the shared channel. Idempotent. A watcher's own bounded buffer
// is released by its pump regardless of whether the consumer drained it.
func (s *Stream) Close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		s.forwards.Wait()
		return
	}
	s.closed = true
	entries := s.watchers
	s.watchers = make(map[WatchID]*streamEntry)
	s.mu.Unlock()

	// Release forwarders blocked on the shared channel before cancelling.
	close(s.stopc)
	for _, entry := range entries {
		entry.w.Cancel()
	}
	s.forwards.Wait()
	close(s.out)
}
