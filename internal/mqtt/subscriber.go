package mqtt

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"strconv"
	"strings"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"idt-worker/internal/model"
)

// roundFloat rounds v to the given number of decimal places.
func roundFloat(v float64, decimals int) float64 {
	if decimals <= 0 {
		return math.Round(v)
	}
	mult := math.Pow(10, float64(decimals))
	return math.Round(v*mult) / mult
}

const (
	TopicRealtime = "machine/+/realtime"
	QoS           = 0
)

// flexTime accepts JSON "time" as either string (RFC3339) or number (Unix seconds).
type flexTime string

func (t *flexTime) UnmarshalJSON(b []byte) error {
	var v interface{}
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	switch x := v.(type) {
	case string:
		*t = flexTime(x)
		return nil
	case float64:
		*t = flexTime(strconv.FormatInt(int64(x), 10))
		return nil
	default:
		return fmt.Errorf("time: expected string or number, got %T", v)
	}
}

// TopicToMachineId extracts machineId from topic "machine/{id}/realtime".
func TopicToMachineId(topic string) (string, bool) {
	parts := strings.Split(topic, "/")
	if len(parts) != 3 || parts[0] != "machine" || parts[2] != "realtime" {
		return "", false
	}
	return parts[1], true
}

// parsePayload unmarshals JSON and reads fields with flexible keys (person/PERSON, volt/Volt, etc.).
// timeStr = from "time" key only, timestampStr = from "timestamp" key only (stored separately).
func parsePayload(raw []byte) (serial, person string, manPower int, jobId, subJob string, stroke int, volt, amp, pf, wh float64, timeStr, timestampStr string, err error) {
	var m map[string]interface{}
	if err = json.Unmarshal(raw, &m); err != nil {
		return "", "", 0, "", "", 0, 0, 0, 0, 0, "", "", err
	}
	getStr := func(keys ...string) string {
		for _, k := range keys {
			if v, ok := m[k]; ok && v != nil {
				if s, ok := v.(string); ok {
					return s
				}
				return fmt.Sprint(v)
			}
		}
		return ""
	}
	getInt := func(keys ...string) int {
		for _, k := range keys {
			if v, ok := m[k]; ok && v != nil {
				switch x := v.(type) {
				case float64:
					return int(x)
				case int:
					return x
				case string:
					n, _ := strconv.Atoi(x)
					return n
				}
			}
		}
		return 0
	}
	getFloat := func(keys ...string) float64 {
		for _, k := range keys {
			if v, ok := m[k]; ok && v != nil {
				switch x := v.(type) {
				case float64:
					return x
				case int:
					return float64(x)
				case string:
					f, _ := strconv.ParseFloat(x, 64)
					return f
				}
			}
		}
		return 0
	}
	getTimeStr := func(keys ...string) string {
		for _, k := range keys {
			if v, ok := m[k]; ok && v != nil {
				switch x := v.(type) {
				case string:
					return x
				case float64:
					return strconv.FormatInt(int64(x), 10)
				case int:
					return strconv.FormatInt(int64(x), 10)
				}
			}
		}
		return ""
	}
	serial = getStr("serial", "Serial", "SERIAL")
	person = getStr("person", "PERSON", "Person")
	manPower = getInt("manPower", "Man Power", "man_power", "MAN_POWER")
	jobId = getStr("jobId", "JOB_ID", "JobId", "job_id")
	subJob = getStr("subJob", "SUB_JOB", "SubJob", "sub_job")
	stroke = getInt("stroke", "Stroke", "STOKE", "stroke")
	volt = getFloat("volt", "Volt", "VOLT")
	amp = getFloat("amp", "Amp", "AMP")
	pf = getFloat("pf", "PF", "Pf")
	wh = getFloat("wh", "Wh", "WH")
	timeStr = getTimeStr("time", "TIME", "Time")           // only "time" keys
	timestampStr = getTimeStr("timestamp", "Timestamp")   // only "timestamp" keys
	return serial, person, manPower, jobId, subJob, stroke, volt, amp, pf, wh, timeStr, timestampStr, nil
}

// Subscribe subscribes to machine/+/realtime and calls onMessage for each message.
func Subscribe(client mqtt.Client, onMessage func(model.MachineData) error) error {
	token := client.Subscribe(TopicRealtime, QoS, func(c mqtt.Client, msg mqtt.Message) {
		topic := msg.Topic()
		log.Printf("mqtt: received topic=%s payload_len=%d", topic, len(msg.Payload()))
		machineId, ok := TopicToMachineId(topic)
		if !ok {
			log.Printf("mqtt: invalid topic %q", msg.Topic())
			return
		}
		serial, person, manPower, jobId, subJob, stroke, volt, amp, pf, wh, timeStr, timestampStr, err := parsePayload(msg.Payload())
		if err != nil {
			log.Printf("mqtt: invalid json topic=%s err=%v", msg.Topic(), err)
			return
		}
		now := time.Now()
		var timeParsed, timestampParsed time.Time
		timeParsed = now
		timestampParsed = now
		if timeStr != "" {
			if parsed, err := parseTime(timeStr); err == nil && parsed != nil {
				timeParsed = *parsed
			} else {
				log.Printf("mqtt: invalid time %q topic=%s using now", timeStr, msg.Topic())
			}
		}
		if timestampStr != "" {
			if parsed, err := parseTime(timestampStr); err == nil && parsed != nil {
				timestampParsed = *parsed
			} else {
				log.Printf("mqtt: invalid timestamp %q topic=%s using now", timestampStr, msg.Topic())
			}
		}
		data := model.MachineData{
			MachineId: machineId,
			Serial:    serial,
			Person:    person,
			ManPower:  manPower,
			JobId:     jobId,
			SubJob:    subJob,
			Stroke:    stroke,
			Volt:      roundFloat(volt, 2),
			Amp:       roundFloat(amp, 2),
			Pf:        roundFloat(pf, 2),
			Wh:        roundFloat(wh, 2),
			Time:      timeParsed,
			Timestamp: timestampParsed,
		}
		if err := onMessage(data); err != nil {
			log.Printf("mqtt: influx write failed topic=%s err=%v", msg.Topic(), err)
		}
	})
	token.Wait()
	if token.Error() != nil {
		return fmt.Errorf("subscribe %s: %w", TopicRealtime, token.Error())
	}
	log.Printf("mqtt: subscribed to %s QoS=%d", TopicRealtime, QoS)
	return nil
}
