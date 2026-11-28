package positions

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/toooriii/tori/internal/repository/trip"
)

type fakeTicker struct {
	ch     chan time.Time
	mu     sync.Mutex
	closed bool
}

func newFakeTicker() *fakeTicker {
	return &fakeTicker{ch: make(chan time.Time, 8)}
}

func (f *fakeTicker) C() <-chan time.Time { return f.ch }

func (f *fakeTicker) Stop() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.closed {
		f.closed = true
		close(f.ch)
	}
}

func (f *fakeTicker) Tick(t time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return
	}
	f.ch <- t
}

type fakeSource struct {
	mu      sync.Mutex
	pages   [][]trip.StaleTrip
	call    int
	blockCh chan struct{}
	calls   int
	entered chan struct{}
	active  atomic.Int64
	overlap atomic.Bool
}

func (f *fakeSource) Stale(_ context.Context, _ time.Duration, _ int, _ *trip.StaleCursor) ([]trip.StaleTrip, error) {
	if f.active.Add(1) > 1 {
		f.overlap.Store(true)
	}
	defer f.active.Add(-1)

	if f.entered != nil {
		select {
		case f.entered <- struct{}{}:
		default:
		}
	}
	if f.blockCh != nil {
		<-f.blockCh
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.call >= len(f.pages) {
		return nil, nil
	}
	page := f.pages[f.call]
	f.call++
	return page, nil
}

func (f *fakeSource) Active() int64 { return f.active.Load() }

func (f *fakeSource) Calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func nopLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(testWriter{}, &slog.HandlerOptions{Level: slog.LevelError}))
}

type testWriter struct{}

func (testWriter) Write(p []byte) (int, error) { return len(p), nil }

func TestScanner_Enqueues(t *testing.T) {
	q := newFIFOQueue(10)
	defer q.Close()
	m, err := newMetrics()
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	src := &fakeSource{pages: [][]trip.StaleTrip{{
		{
			TripID:   uuid.New(),
			DriverID: uuid.New(),
			StaleAt:  now,
		},
		{
			TripID:   uuid.New(),
			DriverID: uuid.New(),
			StaleAt:  now.Add(-time.Minute),
		},
	}}}
	ft := newFakeTicker()

	sc := newScanner(
		src, q, nopLogger(), m,
		time.Minute, 0, 10,
		func(time.Duration) Ticker { return ft },
	)
	sc.tick(context.Background())
	if got := q.Len(); got != 2 {
		t.Fatalf("queue len = %d, want 2", got)
	}
	if got := src.Calls(); got != 1 {
		t.Fatalf("source calls = %d, want 1", got)
	}
}

func TestScanner_Duplicate(t *testing.T) {
	q := newFIFOQueue(10)
	defer q.Close()
	m, err := newMetrics()
	if err != nil {
		t.Fatal(err)
	}
	id := uuid.New()
	src := &fakeSource{pages: [][]trip.StaleTrip{
		{{TripID: id, DriverID: uuid.New(), StaleAt: time.Now()}},
		{{TripID: id, DriverID: uuid.New(), StaleAt: time.Now()}},
	}}
	ft := newFakeTicker()
	sc := newScanner(
		src, q, nopLogger(), m,
		time.Minute, 0, 10,
		func(time.Duration) Ticker { return ft },
	)
	ctx := context.Background()
	sc.tick(ctx)

	if got := q.Len(); got != 1 {
		t.Fatalf("len after first tick = %d, want 1", got)
	}
	sc.tick(ctx)
	if got := q.Len(); got != 1 {
		t.Fatalf("len after second tick = %d, want 1", got)
	}
	if got := src.Calls(); got != 2 {
		t.Fatalf("calls = %d, want 2", got)
	}
}

func TestScanner_Full(t *testing.T) {
	q := newFIFOQueue(1)
	defer q.Close()
	m, err := newMetrics()
	if err != nil {
		t.Fatal(err)
	}
	src := &fakeSource{pages: [][]trip.StaleTrip{{
		{TripID: uuid.New(), DriverID: uuid.New(), StaleAt: time.Now()},
		{TripID: uuid.New(), DriverID: uuid.New(), StaleAt: time.Now()},
	}}}
	ft := newFakeTicker()
	sc := newScanner(
		src, q, nopLogger(), m,
		time.Minute, 0, 1,
		func(time.Duration) Ticker { return ft },
	)
	done := make(chan struct{})
	go func() {
		defer close(done)
		sc.tick(context.Background())
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("scanner blocked on full queue")
	}
	if got := src.Calls(); got != 1 {
		t.Fatalf("calls = %d, want 1", got)
	}
	if got := q.Len(); got != 1 {
		t.Fatalf("queue len = %d, want 1", got)
	}
}

func TestScanner_NoOverlap(t *testing.T) {
	q := newFIFOQueue(10)
	defer q.Close()
	m, err := newMetrics()
	if err != nil {
		t.Fatal(err)
	}
	src := &fakeSource{
		pages: [][]trip.StaleTrip{{
			{
				TripID:   uuid.New(),
				DriverID: uuid.New(),
				StaleAt:  time.Now(),
			},
		}},
		blockCh: make(chan struct{}),
		entered: make(chan struct{}, 2),
	}
	ft := newFakeTicker()
	sc := newScanner(
		src, q, nopLogger(), m,
		time.Minute, 0, 10,
		func(time.Duration) Ticker { return ft },
	)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var unblock sync.Once
	release := func() {
		unblock.Do(func() {
			close(src.blockCh)
		})
	}
	go func() {
		defer close(done)
		sc.run(ctx, time.Hour)
	}()

	defer func() {
		release()
		cancel()
		<-done
	}()
	ft.Tick(time.Now())
	select {
	case <-src.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("first Stale was not called")
	}
	if got := src.Active(); got != 1 {
		t.Fatalf("active = %d, want 1", got)
	}
	ft.Tick(time.Now())
	release()
	select {
	case <-src.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("second Stale was not called")
	}
	cancel()
	<-done

	if src.overlap.Load() {
		t.Fatal("parallel Stale calls detected")
	}

	if got := src.Calls(); got != 2 {
		t.Fatalf("calls = %d, want 2", got)
	}

	if got := src.Active(); got != 0 {
		t.Fatalf("active = %d, want 0", got)
	}
}

func TestScanner_Batches(t *testing.T) {
	q := newFIFOQueue(1000)
	defer q.Close()
	m, _ := newMetrics()

	now := time.Now()
	page1 := make([]trip.StaleTrip, 100)
	page2 := make([]trip.StaleTrip, 100)
	page3 := make([]trip.StaleTrip, 50)
	for i := range page1 {
		page1[i] = trip.StaleTrip{TripID: uuid.New(), DriverID: uuid.New(), StaleAt: now.Add(-time.Duration(i) * time.Second)}
	}
	for i := range page2 {
		page2[i] = trip.StaleTrip{TripID: uuid.New(), DriverID: uuid.New(), StaleAt: now.Add(-time.Duration(100+i) * time.Second)}
	}
	for i := range page3 {
		page3[i] = trip.StaleTrip{TripID: uuid.New(), DriverID: uuid.New(), StaleAt: now.Add(-time.Duration(200+i) * time.Second)}
	}
	src := &fakeSource{pages: [][]trip.StaleTrip{page1, page2, page3}}
	ft := newFakeTicker()
	sc := newScanner(src, q, nopLogger(), m, time.Minute, 100, 1000, func(time.Duration) Ticker { return ft })

	sc.tick(context.Background())
	if got := q.Len(); got != 250 {
		t.Fatalf("queue len = %d, want 250", got)
	}
	if got := src.Calls(); got != 3 {
		t.Fatalf("calls = %d, want 3", got)
	}
}

func TestScanner_BatchesStopOnFullQueue(t *testing.T) {
	q := newFIFOQueue(50)
	defer q.Close()
	m, _ := newMetrics()

	page1 := make([]trip.StaleTrip, 100)
	for i := range page1 {
		page1[i] = trip.StaleTrip{TripID: uuid.New(), DriverID: uuid.New(), StaleAt: time.Now()}
	}
	src := &fakeSource{pages: [][]trip.StaleTrip{page1, page1}}
	ft := newFakeTicker()
	sc := newScanner(src, q, nopLogger(), m, time.Minute, 100, 50, func(time.Duration) Ticker { return ft })

	sc.tick(context.Background())
	if got := q.Len(); got != 50 {
		t.Fatalf("queue len = %d, want 50", got)
	}
	if got := src.Calls(); got != 1 {
		t.Fatalf("calls = %d, want 1", got)
	}
}
