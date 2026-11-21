package positions

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/toooriii/tori/internal/config"
	"github.com/toooriii/tori/internal/repository/trip"
)

type staticSource struct {
	trips []trip.StaleTrip
}

func (s *staticSource) Stale(_ context.Context, _ time.Duration, _ int, _ *trip.StaleCursor) ([]trip.StaleTrip, error) {
	return s.trips, nil
}

func TestComponent_StartStopNoLeak(t *testing.T) {
	cfg := config.Positions{
		CheckInterval: time.Hour,
		StaleAfter:    time.Minute,
		Workers:       2,
		QueueSize:     10,
		PushTimeout:   time.Second,
		BatchSize:     0,
	}
	src := &staticSource{}
	sender := &fakeSender{}
	ft := newFakeTicker()

	comp, err := New(cfg, src, sender, nopLogger(), func(time.Duration) Ticker { return ft })
	if err != nil {
		t.Fatal(err)
	}
	comp.Start(context.Background())
	comp.Stop(context.Background())
}

func TestComponent_Cancel(t *testing.T) {
	cfg := config.Positions{Workers: 1, QueueSize: 10, PushTimeout: time.Second}
	comp, err := New(cfg, &staticSource{}, &fakeSender{}, nopLogger(), func(time.Duration) Ticker { return newFakeTicker() })
	if err != nil {
		t.Fatal(err)
	}
	id := uuid.New()
	if got := comp.Cancel(id); got {
		t.Fatal("cancel on empty queue = true")
	}
}
