// Package quality counts rejected and flagged messages per device, per minute.
// Counts are flushed to InfluxDB measurement machine_ingest_quality so the
// backend completeness report can show them next to the received samples.
package quality

import (
	"sort"
	"sync"
	"time"

	"idt-worker/internal/validate"
)

// maxSample caps the example detail kept per counter (payload fragments can be large).
const maxSample = 200

// Point is one aggregated counter ready to be written.
type Point struct {
	MachineId string // "" when the topic itself was invalid
	Kind      string // reject | flag
	Reason    string
	Minute    time.Time
	N         int64
	Sample    string // last detail seen in that minute
}

type key struct {
	machineId string
	kind      string
	reason    string
	minute    int64
}

// Aggregator is safe for concurrent use.
type Aggregator struct {
	mu     sync.Mutex
	counts map[key]*Point
}

// NewAggregator returns an empty aggregator.
func NewAggregator() *Aggregator {
	return &Aggregator{counts: make(map[key]*Point)}
}

// Add counts each issue in the minute of at.
func (a *Aggregator) Add(machineId string, issues []validate.Issue, at time.Time) {
	if len(issues) == 0 {
		return
	}
	minute := at.UTC().Truncate(time.Minute)
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, is := range issues {
		k := key{machineId, is.Kind, is.Reason, minute.Unix()}
		p, ok := a.counts[k]
		if !ok {
			p = &Point{MachineId: machineId, Kind: is.Kind, Reason: is.Reason, Minute: minute}
			a.counts[k] = p
		}
		p.N++
		if is.Detail != "" {
			p.Sample = truncate(is.Detail, maxSample)
		}
	}
}

// Drain removes and returns counters for minutes strictly before `before`
// (pass the zero time to drain everything, e.g. on shutdown).
func (a *Aggregator) Drain(before time.Time) []Point {
	a.mu.Lock()
	defer a.mu.Unlock()
	var out []Point
	for k, p := range a.counts {
		if before.IsZero() || p.Minute.Before(before) {
			out = append(out, *p)
			delete(a.counts, k)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].Minute.Equal(out[j].Minute) {
			return out[i].Minute.Before(out[j].Minute)
		}
		if out[i].MachineId != out[j].MachineId {
			return out[i].MachineId < out[j].MachineId
		}
		return out[i].Reason < out[j].Reason
	})
	return out
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	// cut on a rune boundary
	for n > 0 && (s[n]&0xC0) == 0x80 {
		n--
	}
	return s[:n]
}
