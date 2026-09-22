package influx

import (
	"context"
	"log"
	"math"
	"sync/atomic"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/influxdata/influxdb-client-go/v2/api/write"

	"idt-worker/internal/model"
	"idt-worker/internal/quality"
)

const (
	measurementRealtime = "machine_realtime"
	measurementQuality  = "machine_ingest_quality"
)

// Options for the batched, retrying write API.
type Options struct {
	BatchSize        uint // points per HTTP write (default 200)
	FlushIntervalMs  uint // max time a point waits in the batch (default 1000)
	RetryBufferLimit uint // points kept for retry while InfluxDB is down (default 100000)
	MaxRetryTimeMs   uint // give up on a batch after this long (default 10 min)
}

// Writer writes samples and quality counters to InfluxDB without blocking the
// MQTT handler. Failed batches are retried from an in-memory buffer; errors
// that exhaust retries are logged and counted.
type Writer struct {
	client influxdb2.Client
	api    api.WriteAPI
	errs   atomic.Int64
	done   chan struct{}
}

// NewWriter creates the client and starts draining the async error channel.
func NewWriter(url, token, org, bucket string, o Options) *Writer {
	if o.BatchSize == 0 {
		o.BatchSize = 200
	}
	if o.FlushIntervalMs == 0 {
		o.FlushIntervalMs = 1000
	}
	if o.RetryBufferLimit == 0 {
		o.RetryBufferLimit = 100_000
	}
	if o.MaxRetryTimeMs == 0 {
		o.MaxRetryTimeMs = 600_000
	}
	opts := influxdb2.DefaultOptions().
		SetBatchSize(o.BatchSize).
		SetFlushInterval(o.FlushIntervalMs).
		SetRetryBufferLimit(o.RetryBufferLimit).
		SetMaxRetryTime(o.MaxRetryTimeMs).
		SetMaxRetries(20)
	client := influxdb2.NewClientWithOptions(url, token, opts)
	w := &Writer{client: client, api: client.WriteAPI(org, bucket), done: make(chan struct{})}

	// The error channel is unbuffered: it must be drained before any write or the API blocks.
	errCh := w.api.Errors()
	go func() {
		defer close(w.done)
		for err := range errCh {
			w.errs.Add(1)
			log.Printf("influx: write error: %v", err)
		}
	}()
	return w
}

// Health checks that InfluxDB is reachable.
func (w *Writer) Health(ctx context.Context) error {
	_, err := w.client.Health(ctx)
	return err
}

// WriteErrors returns the number of write errors reported so far.
func (w *Writer) WriteErrors() int64 { return w.errs.Load() }

// WriteSample queues one realtime sample. Only fields that were actually sent are written.
func (w *Writer) WriteSample(d model.MachineData) {
	w.api.WritePoint(SamplePoint(d))
}

// SamplePoint builds the machine_realtime point. Field types match what the
// previous worker wrote (stroke/manPower as integer, electrical values as float),
// otherwise InfluxDB rejects the write with a field type conflict.
func SamplePoint(d model.MachineData) *write.Point {
	p := influxdb2.NewPointWithMeasurement(measurementRealtime).
		AddTag("machineId", d.MachineId).
		AddTag("serial", d.Serial).
		AddTag("person", d.Person).
		AddTag("jobId", d.JobId).
		AddTag("company", d.Company).
		AddTag("subJob", d.SubJob).
		SetTime(d.Timestamp)

	if d.Working != nil {
		p.AddField("working", *d.Working)
	}
	if d.Status != nil {
		p.AddField("status", *d.Status)
	} else if d.Working != nil {
		// legacy readers use "status" — keep it in step with "working"
		p.AddField("status", *d.Working)
	}
	if d.ManPower != nil {
		p.AddField("manPower", *d.ManPower)
	}
	if d.Stroke != nil {
		p.AddField("stroke", *d.Stroke)
	}
	if d.Volt != nil {
		p.AddField("volt", round2(*d.Volt))
	}
	if d.Amp != nil {
		p.AddField("amp", round2(*d.Amp))
	}
	if d.Pf != nil {
		p.AddField("pf", round2(*d.Pf))
	}
	if d.Ptot != nil {
		p.AddField("ptot", round2(*d.Ptot))
	}
	if d.Wh != nil {
		p.AddField("wh", round2(*d.Wh))
	}
	if d.RunSec != nil {
		p.AddField("run_sec", *d.RunSec)
		// legacy field: the old worker parsed "time" as epoch seconds, i.e. run seconds × 1000
		p.AddField("time_ms", *d.RunSec*1000)
	}
	if d.Seq != nil {
		p.AddField("seq", *d.Seq)
	}
	if d.TsServer {
		p.AddField("ts_server", true)
	}
	p.AddField("timestamp_ms", d.Timestamp.UnixMilli())
	p.AddField("rx_ms", d.ReceivedAt.UnixMilli())
	return p
}

// WriteQuality queues aggregated reject/flag counters.
func (w *Writer) WriteQuality(points []quality.Point) {
	for _, q := range points {
		p := influxdb2.NewPointWithMeasurement(measurementQuality).
			AddTag("kind", q.Kind).
			AddTag("reason", q.Reason).
			AddField("n", q.N).
			SetTime(q.Minute)
		if q.MachineId != "" {
			p.AddTag("machineId", q.MachineId)
		}
		if q.Sample != "" {
			p.AddField("sample", q.Sample)
		}
		w.api.WritePoint(p)
	}
}

// Flush forces all queued points to be written.
func (w *Writer) Flush() { w.api.Flush() }

// Close flushes and releases the client.
func (w *Writer) Close() {
	w.api.Flush()
	w.client.Close()
	select {
	case <-w.done:
	case <-time.After(2 * time.Second):
	}
}

func round2(v float64) float64 {
	return math.Round(v*100) / 100
}
