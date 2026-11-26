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
	"sync"
	"time"

	"github.com/jonboulle/clockwork"
)

// leaseClock is a clockwork.Clock whose Now never moves backwards.
//
// Lease expiry is computed as "now + TTL" and renewed on every keepalive. If
// the wall clock jumps backwards, a renewal would therefore move the expiry
// into the past and the lease would be reclaimed before its owner ever
// observed it expiring. To avoid that, leaseClock never reports a time
// earlier than the latest time it reported before: a backwards jump is
// clamped to the high-water mark until the wall clock catches up.
//
// Clamping can hide a large backwards jump from the expiry logic, so jumps
// beyond warnThreshold are reported through the warn callback. The callback
// is rate limited: it fires when the rollback deepens, not on every call
// while the clock stays behind.
type leaseClock struct {
	clockwork.Clock

	mu            sync.Mutex
	last          time.Time
	warnThreshold time.Duration
	warnedBack    time.Duration
	warn          func(rollback time.Duration)
}

func newLeaseClock(c clockwork.Clock, warnThreshold time.Duration, warn func(time.Duration)) *leaseClock {
	if c == nil {
		c = clockwork.NewRealClock()
	}
	return &leaseClock{
		Clock:         c,
		warnThreshold: warnThreshold,
		warn:          warn,
	}
}

// Now returns the current time, clamped so it never moves backwards.
func (c *leaseClock) Now() time.Time {
	now := c.Clock.Now()

	c.mu.Lock()
	if c.last.IsZero() {
		c.last = now
		c.mu.Unlock()
		return now
	}
	if now.Before(c.last) {
		rollback := c.last.Sub(now)
		last := c.last
		shouldWarn := c.warn != nil && c.warnThreshold > 0 && rollback >= c.warnThreshold && rollback > c.warnedBack
		if shouldWarn {
			c.warnedBack = rollback
		}
		c.mu.Unlock()
		if shouldWarn {
			c.warn(rollback)
		}
		return last
	}
	c.last = now
	c.warnedBack = 0
	c.mu.Unlock()
	return now
}

// Rollback returns how far behind the wall clock currently is, or zero if it
// is at or ahead of the high-water mark.
func (c *leaseClock) Rollback() time.Duration {
	now := c.Clock.Now()

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.last.IsZero() || !now.Before(c.last) {
		return 0
	}
	return c.last.Sub(now)
}

// revokedLeases remembers a bounded set of recently revoked lease IDs.
//
// leaseMap only holds live leases, so once a lease is revoked the lessor can no
// longer tell "already revoked" apart from "never existed". Keeping a bounded
// set of recent revocations lets duplicate Revoke/Renew/Attach calls fail with
// ErrLeaseRevoked instead of the less precise ErrLeaseNotFound, without letting
// the tombstone set grow without bound in a long-running cluster.
type revokedLeases struct {
	ids  map[LeaseID]struct{}
	ring []LeaseID
	next int
}

func newRevokedLeases(capacity int) *revokedLeases {
	if capacity <= 0 {
		capacity = 1
	}
	return &revokedLeases{
		ids:  make(map[LeaseID]struct{}, capacity),
		ring: make([]LeaseID, 0, capacity),
	}
}

func (r *revokedLeases) add(id LeaseID) {
	if _, ok := r.ids[id]; ok {
		return
	}
	if len(r.ring) < cap(r.ring) {
		r.ring = append(r.ring, id)
	} else {
		delete(r.ids, r.ring[r.next])
		r.ring[r.next] = id
		r.next = (r.next + 1) % cap(r.ring)
	}
	r.ids[id] = struct{}{}
}

func (r *revokedLeases) contains(id LeaseID) bool {
	_, ok := r.ids[id]
	return ok
}
