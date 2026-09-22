package mqtt

import (
	"strconv"
	"testing"
	"time"

	"idt-worker/internal/model"
	"idt-worker/internal/quality"
	"idt-worker/internal/validate"
)

func TestHandlerCountsAndWrites(t *testing.T) {
	now := time.Date(2026, 9, 22, 8, 0, 30, 0, time.UTC)
	var written []model.MachineData
	agg := quality.NewAggregator()
	h := &Handler{
		Tracker: validate.NewTracker(),
		Quality: agg,
		Stats:   &Stats{},
		Write:   func(d model.MachineData) { written = append(written, d) },
		Now:     func() time.Time { return now },
	}

	sample := []byte(`{"working":true,"stroke":5,"wh":10,"timestamp":` + strconv.FormatInt(now.Add(-time.Second).UnixMilli(), 10) + `}`)
	h.Handle("machine/D1/realtime", sample)
	h.Handle("machine/D1/realtime", sample) // QoS 1 redelivery
	h.Handle("machine/D1/realtime", []byte(`not json`))
	h.Handle("machine/D1/data", []byte(`{"working":true}`))

	if got := h.Stats.Received.Load(); got != 4 {
		t.Fatalf("received = %d", got)
	}
	if h.Stats.Written.Load() != 2 || h.Stats.Rejected.Load() != 2 || h.Stats.Flagged.Load() != 1 {
		t.Fatalf("stats written=%d rejected=%d flagged=%d", h.Stats.Written.Load(), h.Stats.Rejected.Load(), h.Stats.Flagged.Load())
	}
	if len(written) != 2 || written[0].Stroke == nil || *written[0].Stroke != 5 {
		t.Fatalf("written = %+v", written)
	}

	points := agg.Drain(time.Time{})
	got := map[string]int64{}
	for _, p := range points {
		got[p.MachineId+"|"+p.Kind+"|"+p.Reason] += p.N
		if !p.Minute.Equal(now.Truncate(time.Minute)) {
			t.Fatalf("counter minute = %v", p.Minute)
		}
	}
	want := map[string]int64{
		"D1|flag|duplicate":  1,
		"D1|reject|bad_json": 1,
		"|reject|bad_topic":  1,
	}
	for k, n := range want {
		if got[k] != n {
			t.Errorf("%s = %d, want %d (all: %v)", k, got[k], n, got)
		}
	}
}
