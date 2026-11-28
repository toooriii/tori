package positions

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.uber.org/mock/gomock"
)

func TestPool_ProcessesAll(t *testing.T) {
	q := newFIFOQueue(100)
	defer q.Close()

	const n = 20
	ctrl := gomock.NewController(t)
	sender := NewMockSender(ctrl)
	for i := 0; i < n; i++ {
		j := job(uuid.New(), time.Now())

		if got := q.TryEnqueue(j); got != EnqueueQueued {
			t.Fatalf("enqueue = %v, want queued", got)
		}
		sender.EXPECT().
			SendRequestPosition(gomock.Any(), j.TripID, j.DriverID).
			Return(nil).
			Times(1)
	}
	m, err := newMetrics()
	if err != nil {
		t.Fatal(err)
	}
	p := newPool(q, sender, nopLogger(), m, 4, time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.start(ctx)
	q.Close()
	waitDone(t, &p.wg)

	if got := q.Len(); got != 0 {
		t.Fatalf("queue len = %d, want 0", got)
	}
}

func TestPool_SenderErrorDoesNotKillWorker(t *testing.T) {
	q := newFIFOQueue(10)
	defer q.Close()

	ctrl := gomock.NewController(t)
	sender := NewMockSender(ctrl)

	gomock.InOrder(
		sender.EXPECT().
			SendRequestPosition(gomock.Any(), gomock.Any(), gomock.Any()).
			Return(errors.New("boom")),

		sender.EXPECT().
			SendRequestPosition(gomock.Any(), gomock.Any(), gomock.Any()).
			Return(nil),

		sender.EXPECT().
			SendRequestPosition(gomock.Any(), gomock.Any(), gomock.Any()).
			Return(nil),
	)

	for i := 0; i < 3; i++ {
		if got := q.TryEnqueue(job(uuid.New(), time.Now())); got != EnqueueQueued {
			t.Fatalf("enqueue = %v, want queued", got)
		}
	}
	m, err := newMetrics()
	if err != nil {
		t.Fatal(err)
	}
	p := newPool(q, sender, nopLogger(), m, 1, time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.start(ctx)
	q.Close()
	waitDone(t, &p.wg)
	if got := q.Len(); got != 0 {
		t.Fatalf("queue len = %d, want 0", got)
	}
}

func TestPool_CancelBeforePop(t *testing.T) {
	q := newFIFOQueue(10)
	defer q.Close()
	id := uuid.New()
	if got := q.TryEnqueue(job(id, time.Now())); got != EnqueueQueued {
		t.Fatalf("enqueue = %v, want queued", got)
	}
	if !q.Cancel(id) {
		t.Fatal("cancel = false, want true")
	}
	ctrl := gomock.NewController(t)
	sender := NewMockSender(ctrl)

	sender.EXPECT().
		SendRequestPosition(gomock.Any(), gomock.Any(), gomock.Any()).
		Times(0)

	m, err := newMetrics()
	if err != nil {
		t.Fatal(err)
	}
	p := newPool(q, sender, nopLogger(), m, 2, time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.start(ctx)
	q.Close()
	waitDone(t, &p.wg)
}

func TestPool_Timeout(t *testing.T) {
	q := newFIFOQueue(1)
	defer q.Close()
	j := job(uuid.New(), time.Now())
	if got := q.TryEnqueue(j); got != EnqueueQueued {
		t.Fatalf("enqueue = %v, want queued", got)
	}
	ctrl := gomock.NewController(t)
	sender := NewMockSender(ctrl)
	result := make(chan error, 1)
	sender.EXPECT().
		SendRequestPosition(gomock.Any(), j.TripID, j.DriverID).
		DoAndReturn(func(ctx context.Context, _, _ uuid.UUID) error {
			<-ctx.Done()
			err := ctx.Err()
			result <- err
			return err
		}).
		Times(1)
	m, err := newMetrics()
	if err != nil {
		t.Fatal(err)
	}
	p := newPool(q, sender, nopLogger(), m, 1, 10*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.start(ctx)
	q.Close()
	waitDone(t, &p.wg)
	select {
	case err := <-result:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("error = %v, want DeadlineExceeded", err)
		}
	default:
		t.Fatal("sender did not report timeout")
	}
}

func TestPool_SpanLink(t *testing.T) {
	q := newFIFOQueue(1)
	defer q.Close()
	j := job(uuid.New(), time.Now())
	if got := q.TryEnqueue(j); got != EnqueueQueued {
		t.Fatalf("enqueue = %v, want queued", got)
	}
	ctrl := gomock.NewController(t)
	sender := NewMockSender(ctrl)
	sender.EXPECT().
		SendRequestPosition(gomock.Any(), j.TripID, j.DriverID).
		Return(nil).
		Times(1)
	m, err := newMetrics()
	if err != nil {
		t.Fatal(err)
	}
	p := newPool(q, sender, nopLogger(), m, 1, time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.start(ctx)
	q.Close()
	waitDone(t, &p.wg)
}

func waitDone(t *testing.T, wg *sync.WaitGroup) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("workers did not stop in time")
	}
}
