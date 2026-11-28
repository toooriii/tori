package positions

import (
	"container/heap"
	"context"
	"sync"

	"github.com/google/uuid"
)

type priorityItem struct {
	job   Job
	index int
}

type jobHeap []*priorityItem

func (h jobHeap) Len() int { return len(h) }

func (h jobHeap) Less(i, j int) bool {
	if h[i].job.StaleAt.Equal(h[j].job.StaleAt) {
		return h[i].job.TripID.String() < h[j].job.TripID.String()
	}
	return h[i].job.StaleAt.Before(h[j].job.StaleAt)
}

func (h jobHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *jobHeap) Push(x any) {
	it := x.(*priorityItem) //nolint:forcetypeassert
	it.index = len(*h)
	*h = append(*h, it)
}

func (h *jobHeap) Pop() any {
	old := *h
	n := len(old)
	it := old[n-1]
	old[n-1] = nil
	it.index = -1
	*h = old[:n-1]
	return it
}

type priorityQueue struct {
	mu         sync.Mutex
	cond       *sync.Cond
	h          jobHeap
	in         map[uuid.UUID]*priorityItem
	size       int
	closed     bool
	processing map[uuid.UUID]struct{}
}

func newPriorityQueue(size int) *priorityQueue {
	q := &priorityQueue{
		in:         make(map[uuid.UUID]*priorityItem, size),
		processing: make(map[uuid.UUID]struct{}),
		size:       size,
	}
	q.cond = sync.NewCond(&q.mu)
	return q
}

func (q *priorityQueue) TryEnqueue(j Job) EnqueueResult {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return EnqueueDropped
	}
	if _, ok := q.in[j.TripID]; ok {
		return EnqueueDuplicate
	}
	if _, ok := q.processing[j.TripID]; ok {
		return EnqueueDuplicate
	}
	result := EnqueueQueued
	if len(q.h) >= q.size {
		worst := q.worst()
		if worst == nil || !j.StaleAt.Before(worst.job.StaleAt) {
			return EnqueueDropped
		}
		heap.Remove(&q.h, worst.index)
		delete(q.in, worst.job.TripID)
		result = EnqueueEvicted
	}

	it := &priorityItem{job: j}
	heap.Push(&q.h, it)
	q.in[j.TripID] = it
	q.cond.Signal()

	return result
}

func (q *priorityQueue) worst() *priorityItem {
	if len(q.h) == 0 {
		return nil
	}
	w := q.h[0]
	for _, it := range q.h[1:] {
		if it.job.StaleAt.After(w.job.StaleAt) {
			w = it
		}
	}
	return w
}

func (q *priorityQueue) Cancel(tripID uuid.UUID) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	it, ok := q.in[tripID]
	if !ok {
		return false
	}
	heap.Remove(&q.h, it.index)
	delete(q.in, tripID)
	return true
}

func (q *priorityQueue) Pop(ctx context.Context) (Job, bool) {
	stop := context.AfterFunc(ctx, func() {
		q.mu.Lock()
		q.cond.Broadcast()
		q.mu.Unlock()
	})
	defer stop()

	q.mu.Lock()
	defer q.mu.Unlock()

	for {
		if ctx.Err() != nil {
			return Job{}, false
		}
		if q.closed && len(q.h) == 0 {
			return Job{}, false
		}
		if len(q.h) > 0 {
			it := heap.Pop(&q.h).(*priorityItem) //nolint:forcetypeassert
			delete(q.in, it.job.TripID)
			q.processing[it.job.TripID] = struct{}{}
			return it.job, true
		}
		q.cond.Wait()
	}
}

func (q *priorityQueue) Done(j Job) {
	q.mu.Lock()
	defer q.mu.Unlock()

	delete(q.processing, j.TripID)
}

func (q *priorityQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.h)
}

func (q *priorityQueue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	q.cond.Broadcast()
}
