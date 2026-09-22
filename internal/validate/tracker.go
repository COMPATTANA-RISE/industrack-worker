package validate

import (
	"sync"
	"time"

	"idt-worker/internal/model"
)

// Tracker keeps the last accepted sample per device to flag duplicates,
// out-of-order timestamps and cumulative counters that go down.
// Flags only — the sample is still written (see docs/DEVICE-PAYLOAD-CONTRACT.md §3.3).
type Tracker struct {
	mu      sync.Mutex
	devices map[string]*deviceState
}

type deviceState struct {
	lastTs     time.Time
	lastStroke int64 // 0 = unknown
	lastWh     float64
}

// NewTracker returns an empty tracker.
func NewTracker() *Tracker {
	return &Tracker{devices: make(map[string]*deviceState)}
}

// Check returns sequence issues for d and records it as the device's latest sample.
func (t *Tracker) Check(d *model.MachineData) []Issue {
	t.mu.Lock()
	defer t.mu.Unlock()

	st, ok := t.devices[d.MachineId]
	if !ok {
		st = &deviceState{}
		t.devices[d.MachineId] = st
	}

	var issues []Issue
	older := false
	if !d.TsServer && !st.lastTs.IsZero() {
		switch {
		case d.Timestamp.Equal(st.lastTs):
			issues = append(issues, Issue{KindFlag, ReasonDuplicate, ""})
		case d.Timestamp.Before(st.lastTs):
			older = true
			issues = append(issues, Issue{KindFlag, ReasonOutOfOrder, st.lastTs.Sub(d.Timestamp).String()})
		}
	}

	// Zero readings are ignored here as they are downstream (legacy "missing = 0").
	if !older {
		decreased := ""
		if d.Stroke != nil && *d.Stroke > 0 {
			if st.lastStroke > 0 && *d.Stroke < st.lastStroke {
				decreased = "stroke"
			}
			st.lastStroke = *d.Stroke
		}
		if d.Wh != nil && *d.Wh > 0 {
			if st.lastWh > 0 && *d.Wh < st.lastWh {
				if decreased != "" {
					decreased += ","
				}
				decreased += "wh"
			}
			st.lastWh = *d.Wh
		}
		if decreased != "" {
			issues = append(issues, Issue{KindFlag, ReasonCounterDecrease, decreased})
		}
		if d.Timestamp.After(st.lastTs) {
			st.lastTs = d.Timestamp
		}
	}
	return issues
}
