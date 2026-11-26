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
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/pkg/v3/adt"
)

// DefaultBufferLimit is the per-watcher cap used when WatchOptions does
// not specify one. It matches the historical mvcc watch response channel
// depth; the important difference is that exceeding it now cancels the
// watcher (bounded memory) instead of parking events in an unbounded
// victim list.
const DefaultBufferLimit = 128

// DefaultControlQueueLen sizes the dispatcher's internal control
// channels. Under normal load these never fill; they are bounded only
// so a wedged dispatcher cannot make the process allocate without limit.
const DefaultControlQueueLen = 1024

// Config configures a Bus. The zero value is usable.
type Config struct {
	// Logger defaults to a no-op logger.
	Logger *zap.Logger
	// ControlQueueLen overrides DefaultControlQueueLen when positive.
	ControlQueueLen int
}

// watcherGroup indexes live watchers for point/range matching. It is
// touched only by the dispatcher goroutine and therefore needs no locks.
type watcherGroup struct {
	keyWatchers map[string]map[*subscription]struct{}
	ranges      adt.IntervalTree
	all         map[*subscription]struct{}
	byID        map[WatchID]*subscription
}

func newWatcherGroup() watcherGroup {
	return watcherGroup{
		keyWatchers: make(map[string]map[*subscription]struct{}),
		ranges:      adt.NewIntervalTree(),
		all:         make(map[*subscription]struct{}),
		byID:        make(map[WatchID]*subscription),
	}
}

func (g *watcherGroup) add(w *subscription) {
	g.all[w] = struct{}{}
	g.byID[w.id] = w
	if len(w.end) == 0 {
		k := string(w.key)
		set := g.keyWatchers[k]
		if set == nil {
			set = make(map[*subscription]struct{})
			g.keyWatchers[k] = set
		}
		set[w] = struct{}{}
		return
	}
	ivl := adt.NewStringAffineInterval(string(w.key), string(w.end))
	if iv := g.ranges.Find(ivl); iv != nil {
		iv.Val.(map[*subscription]struct{})[w] = struct{}{}
		return
	}
	g.ranges.Insert(ivl, map[*subscription]struct{}{w: {}})
}

func (g *watcherGroup) delete(w *subscription) bool {
	if _, ok := g.all[w]; !ok {
		return false
	}
	delete(g.all, w)
	delete(g.byID, w.id)
	if len(w.end) == 0 {
		k := string(w.key)
		if set := g.keyWatchers[k]; set != nil {
			delete(set, w)
			if len(set) == 0 {
				delete(g.keyWatchers, k)
			}
		}
		return true
	}
	ivl := adt.NewStringAffineInterval(string(w.key), string(w.end))
	iv := g.ranges.Find(ivl)
	if iv == nil {
		return true
	}
	set := iv.Val.(map[*subscription]struct{})
	delete(set, w)
	if len(set) == 0 {
		g.ranges.Delete(ivl)
	}
	return true
}

// matches returns every watcher interested in key. The returned map is
// freshly allocated and may be iterated while the group mutates.
func (g *watcherGroup) matches(key string) map[*subscription]struct{} {
	hit := make(map[*subscription]struct{})
	if set := g.keyWatchers[key]; set != nil {
		for w := range set {
			hit[w] = struct{}{}
		}
	}
	for _, iv := range g.ranges.Stab(adt.NewStringAffinePoint(key)) {
		for w := range iv.Val.(map[*subscription]struct{}) {
			hit[w] = struct{}{}
		}
	}
	return hit
}

type publishedBatch struct {
	rev int64
	evs []*mvccpb.Event
	err chan error
}

type compactRequest struct {
	revision int64
	err      chan error
}

type watchRequest struct {
	ctx  context.Context
	opts WatchOptions
	resp chan watchResult
}

type watchResult struct {
	w   *Watcher
	err error
}

type progressRequest struct {
	id WatchID
}

// Bus is a bounded-memory, revision-ordered event bus. All routing
// decisions happen on a single dispatcher goroutine, which guarantees
// every watcher observes events in global revision order and means a
// stalled watcher cannot slow dispatch to any other watcher: enqueue is
// non-blocking, and a full per-watcher buffer cancels that watcher only.
type Bus struct {
	lg *zap.Logger

	publishc  chan publishedBatch
	watchc    chan watchRequest
	compactc  chan compactRequest
	removec   chan *subscription
	progressc chan progressRequest
	statsc    chan chan Stats

	stopc  chan struct{}
	closed atomic.Bool

	// pumpWG tracks every watcher pump goroutine so shutdown can wait
	// for all buffers to be released. dispWG tracks the dispatcher
	// itself; they are separate because the dispatcher waits on pumpWG
	// from inside its own goroutine during shutdown.
	pumpWG sync.WaitGroup
	dispWG sync.WaitGroup

	// Fields below are owned exclusively by the dispatcher goroutine.
	group      watcherGroup
	currentRev int64
	compactRev int64
	nextID     WatchID
	stats      Stats
}

// New starts a Bus. Call Close to stop the dispatcher and terminate all
// watchers.
func New(cfg Config) *Bus {
	lg := cfg.Logger
	if lg == nil {
		lg = zap.NewNop()
	}
	queueLen := cfg.ControlQueueLen
	if queueLen <= 0 {
		queueLen = DefaultControlQueueLen
	}
	b := &Bus{
		lg:        lg,
		publishc:  make(chan publishedBatch, queueLen),
		watchc:    make(chan watchRequest, queueLen),
		compactc:  make(chan compactRequest, queueLen),
		removec:   make(chan *subscription, queueLen),
		progressc: make(chan progressRequest, queueLen),
		statsc:    make(chan chan Stats, queueLen),
		stopc:     make(chan struct{}),
		group:     newWatcherGroup(),
		nextID:    1,
	}
	b.dispWG.Add(1)
	go b.run()
	return b
}

// Close terminates the dispatcher and cancels every watcher. It is
// idempotent and blocks until all pump goroutines have released their
// buffers and exited.
func (b *Bus) Close() {
	if !b.closed.CompareAndSwap(false, true) {
		b.dispWG.Wait()
		return
	}
	close(b.stopc)
	// The dispatcher's shutdown waits for all pumps before returning, so
	// waiting on dispWG alone guarantees full cleanup.
	b.dispWG.Wait()
}

// Rev returns the last published revision (0 before the first publish).
func (b *Bus) Rev() int64 {
	// The dispatcher writes currentRev and any goroutine may read it;
	// the value is advisory (progress reporting), so an atomic read keeps
	// the race detector happy without a dispatcher round trip.
	return atomic.LoadInt64(&b.currentRev)
}

// CompactRev returns the revision the store has compacted to. Watchers
// whose next expected revision is at or below it are terminated with
// ErrCompacted.
func (b *Bus) CompactRev() int64 {
	return atomic.LoadInt64(&b.compactRev)
}

// Watch registers a watcher. On success the returned Watcher must
// eventually be drained (read Chan until closed) or cancelled so its
// pump goroutine and buffer can be reclaimed.
func (b *Bus) Watch(ctx context.Context, opts WatchOptions) (*Watcher, error) {
	if b.closed.Load() {
		return nil, ErrBusClosed
	}
	if len(opts.End) != 0 && bytes.Compare(opts.Key, opts.End) != -1 {
		return nil, ErrEmptyWatcherRange
	}
	req := watchRequest{ctx: ctx, opts: opts, resp: make(chan watchResult, 1)}
	select {
	case b.watchc <- req:
	case <-b.stopc:
		return nil, ErrBusClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case res := <-req.resp:
		return res.w, res.err
	case <-b.stopc:
		return nil, ErrBusClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Publish broadcasts one revision's events to every matching watcher.
// rev must be strictly greater than the revision of the previous
// Publish; the events within one batch share rev and are delivered in
// slice order. Publish blocks only while the request is queued to the
// dispatcher, never on a consumer.
func (b *Bus) Publish(rev int64, evs []*mvccpb.Event) error {
	if b.closed.Load() {
		return ErrBusClosed
	}
	if rev <= atomic.LoadInt64(&b.currentRev) {
		return ErrInvalidRevision
	}
	batch := publishedBatch{rev: rev, evs: evs, err: make(chan error, 1)}
	select {
	case b.publishc <- batch:
	case <-b.stopc:
		return ErrBusClosed
	}
	select {
	case err := <-batch.err:
		return err
	case <-b.stopc:
		return ErrBusClosed
	}
}

// Compact advances the compaction revision. Live watchers whose next
// expected revision is at or below revision receive a terminal
// ErrCompacted response carrying revision as CompactRevision.
func (b *Bus) Compact(revision int64) error {
	if b.closed.Load() {
		return ErrBusClosed
	}
	req := compactRequest{revision: revision, err: make(chan error, 1)}
	select {
	case b.compactc <- req:
	case <-b.stopc:
		return ErrBusClosed
	}
	select {
	case err := <-req.err:
		return err
	case <-b.stopc:
		return ErrBusClosed
	}
}

func (b *Bus) run() {
	defer b.dispWG.Done()
	for {
		select {
		case <-b.stopc:
			b.shutdown()
			return
		case req := <-b.watchc:
			b.handleWatch(req)
		case batch := <-b.publishc:
			b.handlePublish(batch)
		case req := <-b.compactc:
			b.handleCompact(req)
		case w := <-b.removec:
			b.handleRemove(w)
		case req := <-b.progressc:
			b.handleProgress(req)
		case resp := <-b.statsc:
			resp <- b.stats
		}
	}
}

func (b *Bus) shutdown() {
	// Every live watcher gets a terminal response; its pump exits via
	// the stopc shortcut even if no consumer is reading.
	for w := range b.group.all {
		w.terminate(WatchResponse{WatchID: w.id, CancelReason: ErrBusClosed})
	}
	b.group = newWatcherGroup()
	// Wait for the pumps spawned in handleWatch. The dispatcher itself
	// contributes the initial Add(1) already consumed by its own Done.
	b.pumpWG.Wait()
}

func (b *Bus) handleWatch(req watchRequest) {
	o := req.opts
	// A start revision at or under the compaction revision can never be
	// served; fail loudly with a classifiable error instead of silently
	// starting at "now".
	if o.StartRev > 0 && o.StartRev <= b.compactRev {
		b.stats.CompactRejects++
		req.resp <- watchResult{err: ErrCompacted}
		return
	}

	id := o.ID
	if id == AutoWatchID {
		for {
			if _, taken := b.group.byID[b.nextID]; !taken {
				id = b.nextID
				b.nextID++
				break
			}
			b.nextID++
		}
	} else if _, taken := b.group.byID[id]; taken {
		req.resp <- watchResult{err: ErrWatcherDuplicateID}
		return
	}

	limit := o.BufferLimit
	if limit <= 0 {
		limit = DefaultBufferLimit
	}

	minRev := o.StartRev
	if minRev <= 0 {
		// Watch from "now": only events published after this point.
		minRev = b.currentRev + 1
	}

	w := &subscription{
		id:       id,
		key:      o.Key,
		end:      o.End,
		minRev:   minRev,
		limit:    limit,
		coalesce: o.Coalesce,
		filters:  o.Filters,
		out:      make(chan WatchResponse, 1),
		wake:     make(chan struct{}, 1),
		done:     make(chan struct{}),
	}
	b.group.add(w)
	b.stats.WatchersCreated++
	b.stats.LiveWatchers = len(b.group.all)

	b.pumpWG.Add(1)
	go func() {
		defer b.pumpWG.Done()
		w.runPump(b)
		// Self-eject once the pump has fully stopped; removal is best
		// effort because shutdown may have cleared the group already.
		select {
		case b.removec <- w:
		case <-b.stopc:
		}
	}()
	w.watchCancelOnContext(req.ctx)
	if o.Timeout > 0 {
		w.watchTimeout(time.Duration(o.Timeout))
	}

	req.resp <- watchResult{w: &Watcher{sub: w, bus: b}}
}

func (b *Bus) handlePublish(batch publishedBatch) {
	b.stats.Publishes++
	b.stats.EventsIn += int64(len(batch.evs))

	// Collect, per matching watcher, the events of this revision in
	// publish order, so each watcher gets one response per revision.
	delivery := make(map[*subscription][]*mvccpb.Event)
	for _, ev := range batch.evs {
		for w := range b.group.matches(string(ev.Kv.Key)) {
			if ev.Kv.ModRevision < w.minRev {
				continue // watcher starts after this revision
			}
			delivery[w] = append(delivery[w], ev)
		}
	}

	for w, evs := range delivery {
		evs = w.applyFilters(evs)
		if len(evs) == 0 {
			// Entire revision filtered out for this watcher.
			continue
		}
		accepted, coalesced := w.enqueueBatch(evs, batch.rev)
		if !accepted {
			if w.isTerminated() {
				// Raced with explicit cancel/timeout/compaction; its
				// self-removal is already pending, do not double-count.
				continue
			}
			b.overflow(w)
			continue
		}
		if coalesced {
			b.stats.Coalesced++
		}
		// minRev tracks the next revision this watcher expects.
		w.minRev = batch.rev + 1
		b.stats.EventsDelivered += int64(len(evs))
	}

	atomic.StoreInt64(&b.currentRev, batch.rev)
	batch.err <- nil
}

func (b *Bus) handleCompact(req compactRequest) {
	if req.revision <= b.compactRev {
		req.err <- nil
		return
	}
	atomic.StoreInt64(&b.compactRev, req.revision)
	for w := range b.group.all {
		if w.minRev <= req.revision {
			b.stats.CompactCancels++
			w.terminate(WatchResponse{
				WatchID:         w.id,
				CompactRevision: req.revision,
				CancelReason:    ErrCompacted,
			})
			b.group.delete(w)
		}
	}
	b.stats.LiveWatchers = len(b.group.all)
	req.err <- nil
}

func (b *Bus) handleRemove(w *subscription) {
	if b.group.delete(w) {
		b.stats.WatchersCanceled++
		b.stats.LiveWatchers = len(b.group.all)
	}
}

// overflow cancels a watcher whose buffer is full and removes it from
// routing. The terminal response states the cause explicitly instead of
// silently dropping events or resetting the connection.
func (b *Bus) overflow(w *subscription) {
	b.stats.OverflowCancels++
	w.terminate(WatchResponse{WatchID: w.id, CancelReason: ErrBufferOverflow})
	b.group.delete(w)
	b.stats.LiveWatchers = len(b.group.all)
	b.lg.Warn("watcher cancelled: buffered event limit exceeded",
		zap.Int64("watch-id", int64(w.id)),
		zap.Int("buffer-limit", w.limit),
		zap.Int("queued", w.queuedEvents()))
}

// handleProgress enqueues a best-effort progress marker. Routing it
// through the dispatcher guarantees the marker is ordered after every
// event the watcher has accepted at this revision.
func (b *Bus) handleProgress(req progressRequest) {
	w := b.group.byID[req.id]
	if w == nil {
		return
	}
	w.requestProgress(b.currentRev)
}

// RequestProgress asks for a zero-event progress response for the
// watcher. The request is best effort: it is dropped if the watcher has
// terminated or its queue is saturated. Returns true when enqueued.
func (b *Bus) RequestProgress(id WatchID) bool {
	if b.closed.Load() {
		return false
	}
	select {
	case b.progressc <- progressRequest{id: id}:
		return true
	case <-b.stopc:
		return false
	}
}

// isTerminated reports whether a terminal response has been staged,
// including the window where the pump is still draining.
func (w *subscription) isTerminated() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.terminated
}
