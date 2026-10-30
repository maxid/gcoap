package gcoap

import (
	"sync"
	"testing"
	"time"
)

// fakeClock is a manual clock for the exchange timers. after registers one
// timer; AdvanceNext moves time to the earliest registered deadline and fires
// it, so retransmission tests never sleep for the configured interval.
type fakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeTimer
}

type fakeTimer struct {
	deadline time.Time
	ch       chan time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Unix(1_700_000_000, 0)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &fakeTimer{deadline: c.now.Add(d), ch: make(chan time.Time, 1)}
	c.timers = append(c.timers, t)
	return t.ch
}

// PendingTimers reports how many timers are registered. The channel-only timer
// primitive has no Stop, so a cancelled exchange leaves its timer behind until
// the next AdvanceNext; tests use this to wait for a new timer.
func (c *fakeClock) PendingTimers() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.timers)
}

// AdvanceNext waits briefly for a timer to be registered, advances the clock to
// its deadline and fires it.
func (c *fakeClock) AdvanceNext(t *testing.T) time.Time {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		c.mu.Lock()
		if len(c.timers) > 0 {
			earliest := c.timers[0].deadline
			for _, timer := range c.timers[1:] {
				if timer.deadline.Before(earliest) {
					earliest = timer.deadline
				}
			}
			if earliest.After(c.now) {
				c.now = earliest
			}
			now := c.now
			// Fire every timer due at this instant. A timer whose exchange
			// finished early is still in the fake list because the channel-only
			// clock has no Stop; firing it is harmless.
			kept := c.timers[:0]
			for _, timer := range c.timers {
				if timer.deadline.After(now) {
					kept = append(kept, timer)
					continue
				}
				timer.ch <- now
			}
			c.timers = kept
			c.mu.Unlock()
			return now
		}
		c.mu.Unlock()
		if time.Now().After(deadline) {
			t.Fatal("no exchange timer was registered")
		}
		time.Sleep(time.Millisecond)
	}
}

// waitFor polls a condition with a short real deadline; it is only a guard
// against a lost wake-up, not a substitute for the manual clock.
func waitFor(t *testing.T, what string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}
