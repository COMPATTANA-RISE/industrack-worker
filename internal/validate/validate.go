// Package validate turns a raw machine/{deviceId}/realtime payload into a
// model.MachineData. Invalid or missing values are dropped (never zero-filled)
// and every problem is reported as an Issue so it can be counted per device.
//
// Rules follow docs/DEVICE-PAYLOAD-CONTRACT.md §3.
package validate

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"idt-worker/internal/model"
)

// Issue kinds.
const (
	KindReject = "reject"
	KindFlag   = "flag"
)

// Issue reasons.
const (
	ReasonBadTopic        = "bad_topic"
	ReasonBadJSON         = "bad_json"
	ReasonBadTimestamp    = "bad_timestamp"
	ReasonEmpty           = "empty"
	ReasonInvalidField    = "invalid_field"
	ReasonTsServer        = "ts_server"
	ReasonFutureTs        = "future_ts"
	ReasonLate            = "late"
	ReasonDuplicate       = "duplicate"
	ReasonOutOfOrder      = "out_of_order"
	ReasonCounterDecrease = "counter_decrease"
)

// Timestamp policies.
const (
	TimestampPolicyServer = "server"
	TimestampPolicyReject = "reject"
)

// Issue is one problem found in a message.
type Issue struct {
	Kind   string
	Reason string
	Detail string
}

// Options tune validation.
type Options struct {
	// TimestampPolicy: "server" (default) uses the receive time when the device
	// timestamp is missing/invalid; "reject" drops the message.
	TimestampPolicy string
	// FutureSkew: device timestamps further ahead of the receive time are
	// replaced by the receive time (flag future_ts). Default 120s.
	FutureSkew time.Duration
	// LateAfter: device timestamps older than the receive time by more than
	// this are kept but flagged late. Default 60s.
	LateAfter time.Duration
}

func (o Options) withDefaults() Options {
	if o.TimestampPolicy == "" {
		o.TimestampPolicy = TimestampPolicyServer
	}
	if o.FutureSkew <= 0 {
		o.FutureSkew = 120 * time.Second
	}
	if o.LateAfter <= 0 {
		o.LateAfter = 60 * time.Second
	}
	return o
}

// Result of validating one message. When Reject is true Data must not be written.
type Result struct {
	Data   model.MachineData
	Issues []Issue
	Reject bool
}

// minValidTime: device clocks that were never set report 1970 — treat anything
// before 2020 as "no timestamp".
var minValidTime = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

// maxRunSec: "time" is working seconds within one job; anything above ~30 years is not.
const maxRunSec = 1_000_000_000

// TopicToMachineId extracts the device id from "machine/{id}/realtime".
func TopicToMachineId(topic string) (string, bool) {
	parts := strings.Split(topic, "/")
	if len(parts) != 3 || parts[0] != "machine" || parts[2] != "realtime" || parts[1] == "" {
		return "", false
	}
	return parts[1], true
}

// Realtime validates one realtime message.
func Realtime(topic string, payload []byte, receivedAt time.Time, opts Options) Result {
	opts = opts.withDefaults()
	var res Result

	machineId, ok := TopicToMachineId(topic)
	if !ok {
		return reject(res, ReasonBadTopic, topic)
	}
	res.Data.MachineId = machineId
	res.Data.ReceivedAt = receivedAt

	f, err := decodeObject(payload)
	if err != nil {
		return reject(res, ReasonBadJSON, err.Error())
	}

	var invalid []string
	d := &res.Data

	d.Serial = f.str("serial")
	d.Person = f.str("person")
	d.SubJob = f.str("subJob", "subJobId")
	if raw := f.str("jobId"); raw != "" {
		d.JobId, d.Company = SplitJobId(raw)
	}

	d.Working = f.boolean(&invalid, "working")
	d.Status = f.boolean(&invalid, "status")
	if d.Working == nil && d.Status != nil {
		w := *d.Status
		d.Working = &w
	}

	d.ManPower = f.count(&invalid, "manPower")
	d.Stroke = f.count(&invalid, "stroke", "stoke")
	d.RunSec = f.count(&invalid, "time")
	if d.RunSec != nil && *d.RunSec > maxRunSec {
		// an epoch timestamp sent in "time" — not a working-seconds counter
		invalid = append(invalid, "time")
		d.RunSec = nil
	}
	d.Seq = f.count(&invalid, "seq")
	d.Volt = f.number(&invalid, false, "volt")
	d.Amp = f.number(&invalid, false, "amp")
	d.Pf = f.number(&invalid, true, "pf")
	d.Ptot = f.number(&invalid, true, "ptot")
	d.Wh = f.number(&invalid, false, "wh")

	if len(invalid) > 0 {
		sort.Strings(invalid)
		res.Issues = append(res.Issues, Issue{KindFlag, ReasonInvalidField, strings.Join(invalid, ",")})
	}

	if d.Working == nil && d.Stroke == nil && d.Wh == nil && d.Volt == nil && d.Amp == nil && d.Pf == nil && d.Ptot == nil {
		return reject(res, ReasonEmpty, "")
	}

	ts, tsOK := f.timestamp("timestamp")
	switch {
	case !tsOK && opts.TimestampPolicy == TimestampPolicyReject:
		return reject(res, ReasonBadTimestamp, f.str("timestamp"))
	case !tsOK:
		d.Timestamp = receivedAt
		d.TsServer = true
		res.Issues = append(res.Issues, Issue{KindFlag, ReasonTsServer, f.str("timestamp")})
	case ts.Sub(receivedAt) > opts.FutureSkew:
		d.Timestamp = receivedAt
		d.TsServer = true
		res.Issues = append(res.Issues, Issue{KindFlag, ReasonFutureTs, ts.UTC().Format(time.RFC3339)})
	default:
		d.Timestamp = ts
		if receivedAt.Sub(ts) > opts.LateAfter {
			res.Issues = append(res.Issues, Issue{KindFlag, ReasonLate, receivedAt.Sub(ts).Round(time.Second).String()})
		}
	}
	return res
}

// SplitJobId splits "JOBCODE#COMPANYCODE" into job code and company code.
func SplitJobId(raw string) (jobCode, company string) {
	raw = strings.TrimSpace(raw)
	if i := strings.Index(raw, "#"); i >= 0 {
		return strings.TrimSpace(raw[:i]), strings.TrimSpace(raw[i+1:])
	}
	return raw, ""
}

func reject(res Result, reason, detail string) Result {
	res.Reject = true
	res.Issues = append(res.Issues, Issue{KindReject, reason, detail})
	return res
}

// fields is a decoded JSON object with keys normalized for lookup, so firmware
// variants such as "Man Power", "man_power", "MAN_POWER" and "manPower" match.
type fields struct {
	exact map[string]any
	norm  map[string]any
}

func decodeObject(payload []byte) (fields, error) {
	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.UseNumber()
	var m map[string]any
	if err := dec.Decode(&m); err != nil {
		return fields{}, err
	}
	if m == nil {
		return fields{}, fmt.Errorf("payload is not a JSON object")
	}
	f := fields{exact: m, norm: make(map[string]any, len(m))}
	for k, v := range m {
		nk := normalizeKey(k)
		if _, dup := f.norm[nk]; !dup {
			f.norm[nk] = v
		}
	}
	return f, nil
}

func normalizeKey(k string) string {
	var b strings.Builder
	for _, r := range k {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(unicode.ToLower(r))
		}
	}
	return b.String()
}

// get returns the first present, non-null value; an exact key wins over a normalized match.
func (f fields) get(keys ...string) (any, string, bool) {
	for _, k := range keys {
		if v, ok := f.exact[k]; ok && v != nil {
			return v, k, true
		}
	}
	for _, k := range keys {
		if v, ok := f.norm[normalizeKey(k)]; ok && v != nil {
			return v, k, true
		}
	}
	return nil, "", false
}

func (f fields) str(keys ...string) string {
	v, _, ok := f.get(keys...)
	if !ok {
		return ""
	}
	switch x := v.(type) {
	case string:
		return strings.TrimSpace(x)
	case json.Number:
		return x.String()
	case bool:
		return strconv.FormatBool(x)
	default:
		return ""
	}
}

func toFloat(v any) (float64, bool) {
	switch x := v.(type) {
	case json.Number:
		n, err := x.Float64()
		if err != nil {
			return 0, false
		}
		return n, true
	case string:
		s := strings.TrimSpace(x)
		if s == "" {
			return 0, false
		}
		n, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return 0, false
		}
		return n, true
	case bool:
		return 0, false
	default:
		return 0, false
	}
}

// number reads a finite float. Negative values are invalid unless allowNegative.
func (f fields) number(invalid *[]string, allowNegative bool, keys ...string) *float64 {
	v, key, ok := f.get(keys...)
	if !ok {
		return nil
	}
	n, ok := toFloat(v)
	if !ok || math.IsNaN(n) || math.IsInf(n, 0) || (!allowNegative && n < 0) {
		*invalid = append(*invalid, key)
		return nil
	}
	return &n
}

// count reads a non-negative integer (fractions are rounded).
func (f fields) count(invalid *[]string, keys ...string) *int64 {
	v, key, ok := f.get(keys...)
	if !ok {
		return nil
	}
	n, ok := toFloat(v)
	if !ok || math.IsNaN(n) || math.IsInf(n, 0) || n < 0 || n > math.MaxInt64/2 {
		*invalid = append(*invalid, key)
		return nil
	}
	i := int64(math.Round(n))
	return &i
}

func (f fields) boolean(invalid *[]string, keys ...string) *bool {
	v, key, ok := f.get(keys...)
	if !ok {
		return nil
	}
	var b bool
	switch x := v.(type) {
	case bool:
		b = x
	case string:
		switch strings.ToLower(strings.TrimSpace(x)) {
		case "true", "1", "on", "running":
			b = true
		case "false", "0", "off", "stopped", "stop":
			b = false
		default:
			*invalid = append(*invalid, key)
			return nil
		}
	case json.Number:
		n, err := x.Float64()
		if err != nil || (n != 0 && n != 1) {
			*invalid = append(*invalid, key)
			return nil
		}
		b = n == 1
	default:
		*invalid = append(*invalid, key)
		return nil
	}
	return &b
}

// timestamp parses epoch milliseconds, epoch seconds or RFC3339.
func (f fields) timestamp(keys ...string) (time.Time, bool) {
	v, _, ok := f.get(keys...)
	if !ok {
		return time.Time{}, false
	}
	var t time.Time
	switch x := v.(type) {
	case string:
		s := strings.TrimSpace(x)
		if parsed, err := time.Parse(time.RFC3339Nano, s); err == nil {
			t = parsed
		} else if n, err := strconv.ParseFloat(s, 64); err == nil {
			t = fromEpoch(n)
		} else {
			return time.Time{}, false
		}
	case json.Number:
		n, err := x.Float64()
		if err != nil {
			return time.Time{}, false
		}
		t = fromEpoch(n)
	default:
		return time.Time{}, false
	}
	if t.Before(minValidTime) {
		return time.Time{}, false
	}
	return t, true
}

// fromEpoch: values above 1e12 are milliseconds (13 digits), otherwise seconds.
func fromEpoch(n float64) time.Time {
	if math.IsNaN(n) || math.IsInf(n, 0) || n <= 0 {
		return time.Time{}
	}
	if n > 1e12 {
		return time.UnixMilli(int64(math.Round(n)))
	}
	sec, frac := math.Modf(n)
	return time.Unix(int64(sec), int64(math.Round(frac*1e9)))
}
