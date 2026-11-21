package positions

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
)

func job(tripID uuid.UUID, staleAt time.Time) Job {
	return Job{TripID: tripID, DriverID: uuid.New(), StaleAt: staleAt}
}

func TestFIFO_Duplicate(t *testing.T) {
	q := newFIFOQueue(10)
	defer q.Close()

	id := uuid.New()
	j := job(id, time.Now())

	if got := q.TryEnqueue(j); got != EnqueueQueued {
		t.Fatalf("first enqueue = %v, want queued", got)
	}
	if got := q.TryEnqueue(j); got != EnqueueDuplicate {
		t.Fatalf("second enqueue = %v, want duplicate", got)
	}
	if q.Len() != 1 {
		t.Fatalf("len = %d, want 1", q.Len())
	}
}

func TestFIFO_Full(t *testing.T) {
	q := newFIFOQueue(1)
	defer q.Close()

	if got := q.TryEnqueue(job(uuid.New(), time.Now())); got != EnqueueQueued {
		t.Fatalf("first = %v, want queued", got)
	}
	if got := q.TryEnqueue(job(uuid.New(), time.Now())); got != EnqueueDropped {
		t.Fatalf("second = %v, want dropped", got)
	}
}

func TestFIFO_CancelBeforePop(t *testing.T) {
	q := newFIFOQueue(10)
	defer q.Close()

	id := uuid.New()
	q.TryEnqueue(job(id, time.Now()))
	if !q.Cancel(id) {
		t.Fatal("cancel = false, want true")
	}
	if q.Len() != 0 {
		t.Fatalf("len = %d, want 0", q.Len())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, ok := q.Pop(ctx); ok {
		t.Fatal("pop returned a job after cancel")
	}
}

func TestFIFO_CloseDrains(t *testing.T) {
	q := newFIFOQueue(10)
	ids := []uuid.UUID{uuid.New(), uuid.New(), uuid.New()}
	for _, id := range ids {
		q.TryEnqueue(job(id, time.Now()))
	}
	q.Close()

	ctx := context.Background()
	for i := 0; i < 3; i++ {
		j, ok := q.Pop(ctx)
		if !ok {
			t.Fatalf("pop %d returned false, want job", i)
		}
		if j.TripID != ids[i] {
			t.Fatalf("pop %d = %s, want %s", i, j.TripID, ids[i])
		}
		q.Done(j)
	}
	if _, ok := q.Pop(ctx); ok {
		t.Fatal("pop after drain returned a job")
	}
}

func TestFIFO_PopContextCancel(t *testing.T) {
	q := newFIFOQueue(1)
	defer q.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, ok := q.Pop(ctx); ok {
		t.Fatal("pop with cancelled ctx returned a job")
	}
}

func TestPriority_Order(t *testing.T) {
	q := newPriorityQueue(10)
	defer q.Close()

	now := time.Now()
	oldest := uuid.New()
	middle := uuid.New()
	newest := uuid.New()

	q.TryEnqueue(job(newest, now.Add(2*time.Minute)))
	q.TryEnqueue(job(oldest, now))
	q.TryEnqueue(job(middle, now.Add(time.Minute)))

	ctx := context.Background()
	want := []uuid.UUID{oldest, middle, newest}
	for i, w := range want {
		j, ok := q.Pop(ctx)
		if !ok {
			t.Fatalf("pop %d returned false", i)
		}
		if j.TripID != w {
			t.Fatalf("pop %d = %s, want %s", i, j.TripID, w)
		}
		q.Done(j)
	}
}

func TestPriority_Evict(t *testing.T) {
	q := newPriorityQueue(2)
	defer q.Close()

	now := time.Now()
	old := uuid.New()
	fresh := uuid.New()
	middle := uuid.New()

	q.TryEnqueue(job(old, now))
	q.TryEnqueue(job(fresh, now.Add(2*time.Minute)))

	if got := q.TryEnqueue(job(middle, now.Add(time.Minute))); got != EnqueueQueued {
		t.Fatalf("enqueue middle = %v, want queued", got)
	}
	if q.Len() != 2 {
		t.Fatalf("len = %d, want 2", q.Len())
	}

	ctx := context.Background()
	first, _ := q.Pop(ctx)
	second, _ := q.Pop(ctx)
	if first.TripID != old || second.TripID != middle {
		t.Fatalf("order = %s, %s; want %s, %s", first.TripID, second.TripID, old, middle)
	}
}

func TestPriority_EvictRejected(t *testing.T) {
	q := newPriorityQueue(2)
	defer q.Close()

	now := time.Now()
	old := uuid.New()
	middle := uuid.New()
	fresh := uuid.New()

	q.TryEnqueue(job(old, now))
	q.TryEnqueue(job(middle, now.Add(time.Minute)))

	if got := q.TryEnqueue(job(fresh, now.Add(2*time.Minute))); got != EnqueueDropped {
		t.Fatalf("enqueue fresh = %v, want dropped", got)
	}
	if q.Len() != 2 {
		t.Fatalf("len = %d, want 2", q.Len())
	}
}

func TestPriority_Cancel(t *testing.T) {
	q := newPriorityQueue(10)
	defer q.Close()

	id := uuid.New()
	q.TryEnqueue(job(id, time.Now()))
	if !q.Cancel(id) {
		t.Fatal("cancel = false, want true")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, ok := q.Pop(ctx); ok {
		t.Fatal("pop returned a job after cancel")
	}
}
