package quality

import (
	"strings"
	"testing"
	"time"

	"idt-worker/internal/validate"
)

func TestAggregatorBucketsByMinuteAndDrainsFinishedMinutes(t *testing.T) {
	a := NewAggregator()
	t0 := time.Date(2026, 9, 22, 8, 0, 10, 0, time.UTC)
	dup := []validate.Issue{{Kind: validate.KindFlag, Reason: validate.ReasonDuplicate}}

	a.Add("D1", dup, t0)
	a.Add("D1", dup, t0.Add(20*time.Second))
	a.Add("D1", dup, t0.Add(70*time.Second)) // next minute
	a.Add("D2", []validate.Issue{{Kind: validate.KindReject, Reason: validate.ReasonBadJSON, Detail: strings.Repeat("é", 300)}}, t0)

	first := a.Drain(t0.Truncate(time.Minute).Add(time.Minute))
	if len(first) != 2 {
		t.Fatalf("first drain = %+v", first)
	}
	for _, p := range first {
		switch p.MachineId {
		case "D1":
			if p.N != 2 {
				t.Fatalf("D1 count = %d", p.N)
			}
		case "D2":
			if len(p.Sample) > maxSample || !strings.HasPrefix(p.Sample, "é") || strings.ContainsRune(p.Sample, '�') {
				t.Fatalf("sample not truncated on a rune boundary: len=%d", len(p.Sample))
			}
		}
	}
	rest := a.Drain(time.Time{})
	if len(rest) != 1 || rest[0].N != 1 {
		t.Fatalf("rest = %+v", rest)
	}
	if len(a.Drain(time.Time{})) != 0 {
		t.Fatal("drain must remove counters")
	}
}
