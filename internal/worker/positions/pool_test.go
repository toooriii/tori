package positions

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
)

type fakeSender struct {
	calls   atomic.Int64
	mu      sync.Mutex
	respond func(tripID uuid.UUID) error
}

func (f *fakeSender) SendRequestPosition(_ context.Context, tripID, _ uuid.UUID) error {
	f.calls.Add(1)
	if f.respond != nil {
		f.mu.Lock()
		fn := f.respond
		f.mu.Unlock()
		return fn(tripID)
	}
	return nil
}

func TestPool_ProcessesAll(t *testing.T) {
	q := newFIFOQueue(100)
	defer q.Close()

	const n = 20
	for i := 0; i < n; i++ {
		q.TryEnqueue(job(uuid.New(), time.Now()))
	}

	sender := &fakeSender{}
	m, _ := newMetrics()
	p := newPool(q, sender, nopLogger(), m, 4, time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	p.start(ctx)

	waitFor(t, func() bool { return q.Len() == 0 && sender.calls.Load() == n })
	q.Close()
	cancel()
	waitDone(t, &p.wg)
}

func TestPool_SenderErrorDoesNotKillWorker(t *testing.T) {
	q := newFIFOQueue(10)
	defer q.Close()

	var once sync.Once
	sender := &fakeSender{}
	sender.respond = func(uuid.UUID) error {
		var err error
		once.Do(func() { err = errors.New("boom") })
		return err
	}
	q.TryEnqueue(job(uuid.New(), time.Now()))
	q.TryEnqueue(job(uuid.New(), time.Now()))
	q.TryEnqueue(job(uuid.New(), time.Now()))

	m, _ := newMetrics()
	p := newPool(q, sender, nopLogger(), m, 2, time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	p.start(ctx)

	waitFor(t, func() bool { return sender.calls.Load() == 3 })
	q.Close()
	cancel()
	waitDone(t, &p.wg)
}

func TestPool_CancelBeforePop(t *testing.T) {
	q := newFIFOQueue(10)
	defer q.Close()

	id := uuid.New()
	q.TryEnqueue(job(id, time.Now()))
	q.Cancel(id)

	sender := &fakeSender{}
	m, _ := newMetrics()
	p := newPool(q, sender, nopLogger(), m, 2, time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	p.start(ctx)

	q.Close()
	cancel()
	waitDone(t, &p.wg)
	if sender.calls.Load() != 0 {
		t.Fatalf("sender called %d times, want 0", sender.calls.Load())
	}
}

func TestPool_Timeout(t *testing.T) {
	q := newFIFOQueue(1)
	defer q.Close()

	q.TryEnqueue(job(uuid.New(), time.Now()))
	sender := &fakeSender{}
	sender.respond = func(uuid.UUID) error {
		time.Sleep(50 * time.Millisecond)
		return context.DeadlineExceeded
	}
	m, _ := newMetrics()
	p := newPool(q, sender, nopLogger(), m, 1, 10*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	p.start(ctx)

	waitFor(t, func() bool { return sender.calls.Load() == 1 })
	q.Close()
	cancel()
	waitDone(t, &p.wg)
}

func TestPool_SpanLink(t *testing.T) {
	q := newFIFOQueue(1)
	defer q.Close()

	q.TryEnqueue(job(uuid.New(), time.Now()))

	var gotDeadline bool
	sender := &fakeSender{}
	sender.respond = func(uuid.UUID) error {
		return nil
	}
	_ = gotDeadline

	m, _ := newMetrics()
	p := newPool(q, sender, nopLogger(), m, 1, time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	p.start(ctx)

	waitFor(t, func() bool { return sender.calls.Load() == 1 })
	q.Close()
	cancel()
	waitDone(t, &p.wg)
}

func waitDone(t *testing.T, wg *sync.WaitGroup) {
	t.Helper()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("workers did not stop in time")
	}
}
