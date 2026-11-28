package positions

import (
	"context"
	"sync"

	"github.com/google/uuid"
)

type fifoQueue struct {
	ch         chan Job
	mu         sync.Mutex
	in         map[uuid.UUID]uint64
	generation uint64
	done       chan struct{}
	once       sync.Once
}

func newFIFOQueue(size int) *fifoQueue {
	return &fifoQueue{
		ch:   make(chan Job, size),
		in:   make(map[uuid.UUID]uint64, size),
		done: make(chan struct{}),
	}
}

func (q *fifoQueue) TryEnqueue(j Job) EnqueueResult {
	q.mu.Lock()
	defer q.mu.Unlock()

	select {
	case <-q.done:
		return EnqueueDropped
	default:
	}

	if _, ok := q.in[j.TripID]; ok {
		return EnqueueDuplicate
	}
	q.generation++
	j.generation = q.generation

	select {
	case q.ch <- j:
		q.in[j.TripID] = j.generation
		return EnqueueQueued
	default:
		return EnqueueDropped
	}
}

func (q *fifoQueue) Cancel(tripID uuid.UUID) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if _, ok := q.in[tripID]; !ok {
		return false
	}
	delete(q.in, tripID)
	return true
}

func (q *fifoQueue) Pop(ctx context.Context) (Job, bool) {
	for {
		select {
		case <-ctx.Done():
			return Job{}, false
		case j := <-q.ch:
			if !q.claimed(j) {
				continue
			}
			return j, true
		case <-q.done:
			for {
				select {
				case <-ctx.Done():
					return Job{}, false

				case j := <-q.ch:
					if !q.claimed(j) {
						continue
					}
					return j, true

				default:
					return Job{}, false
				}
			}
		}
	}
}

func (q *fifoQueue) claimed(j Job) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	generation, ok := q.in[j.TripID]
	return ok && generation == j.generation
}

func (q *fifoQueue) Done(j Job) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if generation, ok := q.in[j.TripID]; ok && generation == j.generation {
		delete(q.in, j.TripID)
	}
}

func (q *fifoQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()

	return len(q.in)
}

func (q *fifoQueue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.once.Do(func() { close(q.done) })
}
