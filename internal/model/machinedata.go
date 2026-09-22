package model

import "time"

// MachineData is one validated realtime sample from MQTT topic machine/{deviceId}/realtime.
//
// Optional values are pointers: nil means "not sent or invalid" and the field is
// omitted from the InfluxDB point instead of being written as 0 — a fake 0 on a
// cumulative counter (stroke, wh) looks like a counter reset downstream.
type MachineData struct {
	MachineId string // topic segment = Machine.deviceId
	Serial    string
	Person    string
	JobId     string // job code (part before '#')
	Company   string // company code (part after '#')
	SubJob    string

	Working *bool // falls back to legacy "status" when "working" is absent
	Status  *bool

	ManPower *int64
	Stroke   *int64   // cumulative counter
	Volt     *float64 // Vavg
	Amp      *float64 // Iavg
	Pf       *float64 // today carries PM2200 Ptot (kW), see docs/DEVICE-PAYLOAD-CONTRACT.md
	Ptot     *float64 // v1.1: total active power (kW)
	Wh       *float64 // cumulative energy counter
	RunSec   *int64   // payload "time": working seconds in the current job
	Seq      *int64   // v1.1: message counter

	Timestamp  time.Time // point time: device timestamp, or receive time when TsServer
	TsServer   bool      // true when the device timestamp was missing/invalid
	ReceivedAt time.Time
}
