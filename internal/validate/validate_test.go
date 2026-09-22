package validate

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"idt-worker/internal/model"
)

var rx = time.Date(2026, 9, 22, 8, 0, 0, 0, time.UTC)

func reasons(is []Issue) string {
	var out []string
	for _, i := range is {
		out = append(out, i.Kind+":"+i.Reason)
	}
	return strings.Join(out, " ")
}

func TestRealtimeRejects(t *testing.T) {
	cases := []struct {
		name, topic, payload, want string
		opts                       Options
	}{
		{"bad topic", "machine/abc/data", `{"working":true}`, "reject:bad_topic", Options{}},
		{"empty device id", "machine//realtime", `{"working":true}`, "reject:bad_topic", Options{}},
		{"bad json", "machine/D1/realtime", `{"working":`, "reject:bad_json", Options{}},
		{"not an object", "machine/D1/realtime", `[1,2]`, "reject:bad_json", Options{}},
		{"no measurements", "machine/D1/realtime", `{"person":"a","timestamp":1790000000000}`, "reject:empty", Options{}},
		{"reject policy", "machine/D1/realtime", `{"working":true}`, "reject:bad_timestamp", Options{TimestampPolicy: TimestampPolicyReject}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := Realtime(c.topic, []byte(c.payload), rx, c.opts)
			if !r.Reject {
				t.Fatalf("expected reject, got %+v", r)
			}
			if got := reasons(r.Issues); !strings.Contains(got, c.want) {
				t.Fatalf("issues = %q, want %q", got, c.want)
			}
		})
	}
}

func TestRealtimeParsesCurrentFirmwarePayload(t *testing.T) {
	payload := `{"person":"sajo","manPower":4,"jobId":"SB6902258#AB12","stroke":699,"time":842,
		"volt":381.51,"amp":8.59,"pf":1.12,"wh":8284,"status":false,"serial":"SBX-1","timestamp":1790000000}`
	r := Realtime("machine/DEV123/realtime", []byte(payload), time.Unix(1790000001, 0), Options{})
	if r.Reject || len(r.Issues) != 0 {
		t.Fatalf("unexpected result: reject=%v issues=%s", r.Reject, reasons(r.Issues))
	}
	d := r.Data
	if d.MachineId != "DEV123" || d.JobId != "SB6902258" || d.Company != "AB12" || d.Person != "sajo" || d.Serial != "SBX-1" {
		t.Fatalf("tags: %+v", d)
	}
	if *d.Stroke != 699 || *d.Wh != 8284 || *d.Pf != 1.12 || *d.ManPower != 4 || *d.RunSec != 842 {
		t.Fatalf("fields: %+v", d)
	}
	// "working" missing → falls back to legacy "status"
	if d.Working == nil || *d.Working != false || d.Status == nil {
		t.Fatalf("working/status: %+v %+v", d.Working, d.Status)
	}
	// epoch seconds accepted
	if !d.Timestamp.Equal(time.Unix(1790000000, 0)) || d.TsServer {
		t.Fatalf("timestamp = %v tsServer=%v", d.Timestamp, d.TsServer)
	}
}

func TestRealtimeKeyVariants(t *testing.T) {
	payload := `{"PERSON":"x","Man Power":"3","JOB_ID":"J1#C1","SUB_JOB":"S1","STOKE":10,"VOLT":"380.5","WH":"100","WORKING":"true","timestamp":1790000000123}`
	r := Realtime("machine/D1/realtime", []byte(payload), rx.Add(time.Hour*10000), Options{})
	if r.Reject {
		t.Fatalf("rejected: %s", reasons(r.Issues))
	}
	d := r.Data
	if *d.ManPower != 3 || *d.Stroke != 10 || *d.Volt != 380.5 || *d.Wh != 100 || !*d.Working || d.SubJob != "S1" || d.JobId != "J1" {
		t.Fatalf("variants not parsed: %+v", d)
	}
	if d.Timestamp.UnixMilli() != 1790000000123 {
		t.Fatalf("ms timestamp = %d", d.Timestamp.UnixMilli())
	}
}

func TestRealtimeInvalidFieldsAreDroppedNotZeroed(t *testing.T) {
	payload := `{"working":true,"stroke":-5,"wh":"NaN","volt":"abc","amp":2.5,"manPower":"many","timestamp":1790000000000}`
	r := Realtime("machine/D1/realtime", []byte(payload), time.UnixMilli(1790000000500), Options{})
	if r.Reject {
		t.Fatalf("should not reject: %s", reasons(r.Issues))
	}
	d := r.Data
	if d.Stroke != nil || d.Wh != nil || d.Volt != nil || d.ManPower != nil {
		t.Fatalf("invalid fields must be nil, got %+v", d)
	}
	if d.Amp == nil || *d.Amp != 2.5 {
		t.Fatalf("valid amp lost: %+v", d.Amp)
	}
	got := reasons(r.Issues)
	if got != "flag:invalid_field" {
		t.Fatalf("issues = %q", got)
	}
	if r.Issues[0].Detail != "manPower,stroke,volt,wh" {
		t.Fatalf("detail = %q", r.Issues[0].Detail)
	}
}

func TestRealtimeTimestampRules(t *testing.T) {
	cases := []struct {
		name      string
		ts        string
		wantFlag  string
		tsServer  bool
		wantPoint time.Time
	}{
		{"missing", ``, "flag:ts_server", true, rx},
		{"never set clock (1970)", `,"timestamp":12345`, "flag:ts_server", true, rx},
		{"garbage", `,"timestamp":"yesterday"`, "flag:ts_server", true, rx},
		{"future", `,"timestamp":` + ms(rx.Add(10*time.Minute)), "flag:future_ts", true, rx},
		{"small future skew ok", `,"timestamp":` + ms(rx.Add(30*time.Second)), "", false, rx.Add(30 * time.Second)},
		{"late", `,"timestamp":` + ms(rx.Add(-5*time.Minute)), "flag:late", false, rx.Add(-5 * time.Minute)},
		{"rfc3339", `,"timestamp":"2026-09-22T07:59:58Z"`, "", false, rx.Add(-2 * time.Second)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := Realtime("machine/D1/realtime", []byte(`{"working":true`+c.ts+`}`), rx, Options{})
			if r.Reject {
				t.Fatalf("rejected: %s", reasons(r.Issues))
			}
			if got := reasons(r.Issues); got != c.wantFlag {
				t.Fatalf("issues = %q, want %q", got, c.wantFlag)
			}
			if r.Data.TsServer != c.tsServer || !r.Data.Timestamp.Equal(c.wantPoint) {
				t.Fatalf("point time = %v (server=%v), want %v (server=%v)", r.Data.Timestamp, r.Data.TsServer, c.wantPoint, c.tsServer)
			}
		})
	}
}

func TestRealtimeTimeFieldHoldingEpochIsInvalid(t *testing.T) {
	r := Realtime("machine/D1/realtime", []byte(`{"working":true,"time":1790000000000,"timestamp":`+ms(rx)+`}`), rx, Options{})
	if r.Data.RunSec != nil || reasons(r.Issues) != "flag:invalid_field" {
		t.Fatalf("runSec=%v issues=%s", r.Data.RunSec, reasons(r.Issues))
	}
}

func TestSplitJobId(t *testing.T) {
	for in, want := range map[string][2]string{
		"SB6902258#AB12": {"SB6902258", "AB12"},
		"SB6902258#":     {"SB6902258", ""},
		" JOB-1 # C1 ":   {"JOB-1", "C1"},
		"NOCOMPANY":      {"NOCOMPANY", ""},
		"A#B#C":          {"A", "B#C"},
	} {
		j, c := SplitJobId(in)
		if j != want[0] || c != want[1] {
			t.Errorf("SplitJobId(%q) = %q,%q want %q,%q", in, j, c, want[0], want[1])
		}
	}
}

func TestTracker(t *testing.T) {
	tr := NewTracker()
	i64 := func(v int64) *int64 { return &v }
	f64 := func(v float64) *float64 { return &v }
	at := func(sec int) time.Time { return rx.Add(time.Duration(sec) * time.Second) }
	check := func(d model.MachineData) string { return reasons(tr.Check(&d)) }

	if got := check(model.MachineData{MachineId: "D1", Timestamp: at(0), Stroke: i64(10), Wh: f64(100)}); got != "" {
		t.Fatalf("first sample flagged: %s", got)
	}
	if got := check(model.MachineData{MachineId: "D1", Timestamp: at(0), Stroke: i64(10)}); got != "flag:duplicate" {
		t.Fatalf("duplicate: %q", got)
	}
	if got := check(model.MachineData{MachineId: "D1", Timestamp: at(-5), Stroke: i64(3)}); got != "flag:out_of_order" {
		t.Fatalf("out of order must not also flag counters: %q", got)
	}
	// zero readings are ignored (legacy missing = 0)
	if got := check(model.MachineData{MachineId: "D1", Timestamp: at(1), Stroke: i64(0), Wh: f64(0)}); got != "" {
		t.Fatalf("zero flagged: %q", got)
	}
	if got := check(model.MachineData{MachineId: "D1", Timestamp: at(2), Stroke: i64(4), Wh: f64(90)}); got != "flag:counter_decrease" {
		t.Fatalf("decrease: %q", got)
	}
	// another device has its own state
	if got := check(model.MachineData{MachineId: "D2", Timestamp: at(0), Stroke: i64(1)}); got != "" {
		t.Fatalf("D2: %q", got)
	}
	// server-stamped samples are never duplicates
	if got := check(model.MachineData{MachineId: "D1", Timestamp: at(2), TsServer: true, Stroke: i64(5)}); got != "" {
		t.Fatalf("ts_server sample: %q", got)
	}
}

func ms(t time.Time) string {
	return strconv.FormatInt(t.UnixMilli(), 10)
}
