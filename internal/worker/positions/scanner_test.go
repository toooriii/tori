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
}

func (f *fakeSource) Stale(_ context.Context, _ time.Duration, _ int, _ *trip.StaleCursor) ([]trip.StaleTrip, error) {
	f.active.Add(1)
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
		{TripID: uuid.New(), DriverID: uuid.New(), StaleAt: now},
		{TripID: uuid.New(), DriverID: uuid.New(), StaleAt: now.Add(-time.Minute)},
	}}}
	ft := newFakeTicker()
	sc := newScanner(src, q, nopLogger(), m, time.Minute, 0, 10, func(time.Duration) Ticker { return ft })

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		sc.run(ctx, time.Hour)
	}()

	ft.Tick(time.Now())
	deadline := time.After(2 * time.Second)
	for q.Len() < 2 {
		select {
		case <-deadline:
			t.Fatalf("queue len = %d, want 2", q.Len())
		default:
			time.Sleep(time.Millisecond)
		}
	}
	cancel()
	<-done
}

func TestScanner_Duplicate(t *testing.T) {
	q := newFIFOQueue(10)
	defer q.Close()
	m, _ := newMetrics()

	id := uuid.New()
	src := &fakeSource{pages: [][]trip.StaleTrip{
		{{TripID: id, DriverID: uuid.New(), StaleAt: time.Now()}},
		{{TripID: id, DriverID: uuid.New(), StaleAt: time.Now()}},
	}}
	ft := newFakeTicker()
	sc := newScanner(src, q, nopLogger(), m, time.Minute, 0, 10, func(time.Duration) Ticker { return ft })

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); sc.run(ctx, time.Hour) }()

	ft.Tick(time.Now())
	waitFor(t, func() bool { return q.Len() == 1 })
	ft.Tick(time.Now())
	waitFor(t, func() bool { return src.Calls() == 2 })
	if q.Len() != 1 {
		t.Fatalf("len = %d, want 1", q.Len())
	}
	cancel()
	<-done
}

func TestScanner_Full(t *testing.T) {
	q := newFIFOQueue(1)
	defer q.Close()
	m, _ := newMetrics()

	src := &fakeSource{pages: [][]trip.StaleTrip{{
		{TripID: uuid.New(), DriverID: uuid.New(), StaleAt: time.Now()},
		{TripID: uuid.New(), DriverID: uuid.New(), StaleAt: time.Now()},
	}}}
	ft := newFakeTicker()
	sc := newScanner(src, q, nopLogger(), m, time.Minute, 0, 1, func(time.Duration) Ticker { return ft })

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); sc.run(ctx, time.Hour) }()

	ft.Tick(time.Now())
	waitFor(t, func() bool { return src.Calls() == 1 })
	waitFor(t, func() bool { return q.Len() == 1 })
	cancel()
	<-done
}

func TestScanner_NoOverlap(t *testing.T) {
	q := newFIFOQueue(10)
	defer q.Close()
	m, _ := newMetrics()

	src := &fakeSource{
		pages:   [][]trip.StaleTrip{{{TripID: uuid.New(), DriverID: uuid.New(), StaleAt: time.Now()}}},
		blockCh: make(chan struct{}),
		entered: make(chan struct{}, 1),
	}
	ft := newFakeTicker()
	sc := newScanner(src, q, nopLogger(), m, time.Minute, 0, 10, func(time.Duration) Ticker { return ft })

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); sc.run(ctx, time.Hour) }()

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

	deadline := time.Now().Add(50 * time.Millisecond)
	for time.Now().Before(deadline) {
		if got := src.Active(); got != 1 {
			t.Fatalf("parallel Stale detected: active = %d, want 1", got)
		}
		time.Sleep(time.Millisecond)
	}

	close(src.blockCh)

	waitFor(t, func() bool { return src.Calls() == 2 })
	if got := src.Active(); got != 0 {
		t.Fatalf("active = %d after all ticks, want 0", got)
	}

	cancel()
	<-done
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

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); sc.run(ctx, time.Hour) }()

	ft.Tick(time.Now())
	waitFor(t, func() bool { return q.Len() == 250 })
	if src.Calls() != 3 {
		t.Fatalf("calls = %d, want 3", src.Calls())
	}
	cancel()
	<-done
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

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); sc.run(ctx, time.Hour) }()

	ft.Tick(time.Now())
	waitFor(t, func() bool { return q.Len() == 50 })
	if src.Calls() != 1 {
		t.Fatalf("calls = %d, want 1 (stop after first batch)", src.Calls())
	}
	cancel()
	<-done
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met in time")
		}
		time.Sleep(time.Millisecond)
	}
}
