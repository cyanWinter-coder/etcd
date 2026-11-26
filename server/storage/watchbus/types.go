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

// Package watchbus is an in-process, bounded-memory pub/sub bus for
// revisioned key events. It is designed to replace the unbounded "victim"
// buffering pattern used by the mvcc watchable store: a slow consumer can
// only hold on to a fixed number of events before it is explicitly
// cancelled, so one stalled watcher can never grow the server's memory
// without limit or block delivery to any other watcher.
//
// The package guarantees:
//
//   - Events are dispatched to every watcher in strict revision order.
//   - A watcher whose consumer is stalled never blocks any other watcher
//     and never blocks Publish; its per-watcher buffer is hard-capped.
//   - Overflow, timeout, context cancellation and compaction each produce
//     a distinct, machine-readable terminal error on the watcher.
//   - Cancellation is idempotent and releases all buffered events.
//   - Same-key consecutive PUT events can optionally be coalesced, while
//     preserving every revision boundary that carries an observable
//     state transition (deletes, create/tombstone boundaries).
package watchbus

import (
	"errors"

	"go.etcd.io/etcd/api/v3/mvccpb"
)

// WatchID identifies a watcher within a bus (and within a stream).
type WatchID int64

// AutoWatchID and InvalidWatchID mirror clientv3's constants so that
// callers ported from mvcc.WatchStream keep working unchanged.
const (
	AutoWatchID    WatchID = 0
	InvalidWatchID WatchID = -1
)

// Terminal errors delivered to watchers. They are sentinel values so
// callers can use errors.Is against the CancelReason of a terminal
// WatchResponse instead of matching human-readable strings.
var (
	// ErrBufferOverflow is returned when a watcher's pending event queue
	// exceeds its configured BufferLimit. The watcher is cancelled rather
	// than allowed to accumulate events indefinitely.
	ErrBufferOverflow = errors.New("watchbus: watcher buffer overflow, slow consumer cancelled")

	// ErrCompacted is returned when the requested start revision has
	// already been compacted, or when a live watcher falls behind the
	// compaction revision. The CompactRevision field of the terminal
	// WatchResponse carries the revision the store compacted to.
	ErrCompacted = errors.New("watchbus: start revision has been compacted")

	// ErrWatcherCanceled is returned on explicit cancellation.
	ErrWatcherCanceled = errors.New("watchbus: watcher canceled")

	// ErrWatcherTimeout is returned when the watcher's configured Timeout
	// elapses (an optional hard lifetime for ephemeral watchers).
	ErrWatcherTimeout = errors.New("watchbus: watcher timed out")

	// ErrBusClosed is returned by Watch/Publish after the bus has been
	// shut down.
	ErrBusClosed = errors.New("watchbus: bus closed")

	// ErrWatcherNotExist is returned when cancelling an ID that is not
	// (or is no longer) registered on a stream.
	ErrWatcherNotExist = errors.New("watchbus: watcher does not exist")

	// ErrWatcherDuplicateID is returned when creating a watcher with an
	// explicit ID already in use on the same stream.
	ErrWatcherDuplicateID = errors.New("watchbus: duplicate watch ID on stream")

	// ErrEmptyWatcherRange is returned when key >= end lexicographically.
	ErrEmptyWatcherRange = errors.New("watchbus: watcher range is empty")

	// ErrInvalidRevision is returned by Publish when the revision does
	// not advance monotonically.
	ErrInvalidRevision = errors.New("watchbus: publish revision must be greater than previous revision")
)

// FilterFunc returns true if the given event should be filtered out for
// a particular watcher. It mirrors mvcc.FilterFunc.
type FilterFunc func(e *mvccpb.Event) bool

// WatchOptions configures a new watcher. The zero value is valid and
// matches the historical mvcc semantics: bounded by DefaultBufferLimit,
// no coalescing, no timeout.
type WatchOptions struct {
	// ID is the requested watcher ID. Leave as AutoWatchID (0) to let
	// the bus allocate one. An explicit ID already in use makes Watch
	// fail with ErrWatcherDuplicateID.
	ID WatchID

	// Key and End define the watched range [Key, End). A nil End means
	// the watcher observes a single key.
	Key []byte
	End []byte

	// StartRev is the first revision the watcher is interested in.
	// StartRev <= 0 means "events after the current revision", matching
	// mvcc's Watch semantics.
	StartRev int64

	// BufferLimit is the hard cap on events queued for this watcher
	// (events held in its in-memory queue plus the slot in its output
	// channel). Zero selects DefaultBufferLimit.
	BufferLimit int

	// Coalesce, when true, merges consecutive PUT events for the same
	// key inside the watcher's pending queue. Events that have already
	// been handed to the consumer are never rewritten, and deletes as
	// well as create/tombstone boundaries are always preserved. Default
	// (false) keeps the exact per-event mvcc delivery semantics.
	Coalesce bool

	// Timeout, when > 0, sets a hard lifetime for the watcher. On expiry
	// the watcher terminates with ErrWatcherTimeout. Zero means no
	// timeout; the watcher then lives until cancellation, compaction,
	// overflow or bus close.
	Timeout int64

	// Filters are applied before an event enters the watcher's queue,
	// so filtered events consume no buffer capacity.
	Filters []FilterFunc
}

// WatchResponse is one delivery on a watcher's output channel.
//
// A response is terminal when CancelReason != nil. Terminal responses
// carry no events; CompactRevision is populated for ErrCompacted.
type WatchResponse struct {
	// WatchID is the ID of the watcher this response belongs to.
	WatchID WatchID

	// Events is a revision-ordered batch of matched events.
	Events []*mvccpb.Event

	// Revision is the bus revision at which this response was produced.
	Revision int64

	// CompactRevision is set for an ErrCompacted terminal response and
	// holds the revision the store compacted to (the first still
	// available revision).
	CompactRevision int64

	// CancelReason is nil for ordinary event and progress responses.
	// For a terminal response it is one of the package's sentinel
	// errors; use errors.Is to classify it.
	CancelReason error
}
