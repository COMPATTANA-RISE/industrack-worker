package influx

import (
	"context"
	"fmt"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"idt-worker/internal/model"
)

const measurement = "machine_realtime"

// Writer writes MachineData to InfluxDB.
type Writer struct {
	client influxdb2.Client
	api    api.WriteAPIBlocking
}

// NewWriter creates an InfluxDB write API client. Caller should call Close() when done.
func NewWriter(url, token, org, bucket string) (*Writer, error) {
	client := influxdb2.NewClient(url, token)
	writeAPI := client.WriteAPIBlocking(org, bucket)
	return &Writer{client: client, api: writeAPI}, nil
}

// Close releases the InfluxDB client.
func (w *Writer) Close() {
	w.client.Close()
}

// Health checks that InfluxDB is reachable and the token is valid.
func (w *Writer) Health(ctx context.Context) error {
	_, err := w.client.Health(ctx)
	return err
}

// Write saves one MachineData point. Point time = Timestamp (or Time if zero); time and timestamp stored as separate fields.
func (w *Writer) Write(ctx context.Context, d model.MachineData) error {
	pointTime := d.Timestamp
	if pointTime.IsZero() {
		pointTime = d.Time
	}
	if pointTime.IsZero() {
		pointTime = time.Now()
	}
	p := influxdb2.NewPointWithMeasurement(measurement).
		AddTag("machineId", d.MachineId).
		AddTag("serial", d.Serial).
		AddTag("person", d.Person).
		AddTag("jobId", d.JobId).
		AddTag("company", d.Company).
		AddTag("subJob", d.SubJob).
		AddField("working", d.Working).
		AddField("status", d.Status).
		AddField("manPower", d.ManPower).
		AddField("stroke", d.Stroke).
		AddField("volt", d.Volt).
		AddField("amp", d.Amp).
		AddField("pf", d.Pf).
		AddField("wh", d.Wh).
		AddField("status", d.Status).
		AddField("time_ms", d.Time.UnixMilli()).
		AddField("timestamp_ms", d.Timestamp.UnixMilli()).
		SetTime(pointTime)
	if err := w.api.WritePoint(ctx, p); err != nil {
		return fmt.Errorf("influx write: %w", err)
	}
	return nil
}
