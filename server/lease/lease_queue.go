// Copyright 2018 The etcd Authors
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
	"time"
)

// LeaseWithTime contains lease object with a time.
// For the lessor's lease heap, time identifies the lease expiration time.
// For the lessor's lease checkpoint heap, the time identifies the next lease checkpoint time.
type LeaseWithTime struct {
	id    LeaseID
	time  time.Time
	index int
}

type LeaseQueue []*LeaseWithTime

func (pq LeaseQueue) Len() int { return len(pq) }

func (pq LeaseQueue) Less(i, j int) bool {
	if pq[i].time.Equal(pq[j].time) {
		// Break ties by lease ID so the heap order is deterministic. Without
		// it, entries with equal expiry can be ordered arbitrarily, which
		// makes batch contents (and tests) unstable.
		return pq[i].id < pq[j].id
	}
	return pq[i].time.Before(pq[j].time)
}

func (pq LeaseQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *LeaseQueue) Push(x any) {
	n := len(*pq)
	item := x.(*LeaseWithTime)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *LeaseQueue) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

// LeaseExpiredNotifier is a queue used to notify lessor to revoke expired lease.
// Only save one item for a lease, `Register` will update time of the corresponding lease.
type LeaseExpiredNotifier struct {
	m     map[LeaseID]*LeaseWithTime
	queue LeaseQueue
}

func newLeaseExpiredNotifier() *LeaseExpiredNotifier {
	return &LeaseExpiredNotifier{
		m:     make(map[LeaseID]*LeaseWithTime),
		queue: make(LeaseQueue, 0),
	}
}

func (mq *LeaseExpiredNotifier) Init() {
	heap.Init(&mq.queue)
	mq.m = make(map[LeaseID]*LeaseWithTime)
	for _, item := range mq.queue {
		mq.m[item.id] = item
	}
}

func (mq *LeaseExpiredNotifier) RegisterOrUpdate(item *LeaseWithTime) {
	if old, ok := mq.m[item.id]; ok {
		old.time = item.time
		heap.Fix(&mq.queue, old.index)
	} else {
		heap.Push(&mq.queue, item)
		mq.m[item.id] = item
	}
}

// Update moves an existing item to the given expiry. Unlike RegisterOrUpdate
// it does not add an item for a lease that is not queued, so a renew can never
// produce two heap entries for one lease.
func (mq *LeaseExpiredNotifier) Update(id LeaseID, time time.Time) bool {
	old, ok := mq.m[id]
	if !ok {
		return false
	}
	old.time = time
	heap.Fix(&mq.queue, old.index)
	return true
}

func (mq *LeaseExpiredNotifier) Unregister() *LeaseWithTime {
	item := heap.Pop(&mq.queue).(*LeaseWithTime)
	delete(mq.m, item.id)
	return item
}

// Remove drops the item for the given lease from the queue, if present.
func (mq *LeaseExpiredNotifier) Remove(id LeaseID) {
	item, ok := mq.m[id]
	if !ok {
		return
	}
	heap.Remove(&mq.queue, item.index)
	delete(mq.m, id)
}

func (mq *LeaseExpiredNotifier) Peek() *LeaseWithTime {
	if mq.Len() == 0 {
		return nil
	}
	return mq.queue[0]
}

func (mq *LeaseExpiredNotifier) Len() int {
	return len(mq.m)
}
