package mqtt

import (
	"fmt"
	"log"
	"sync/atomic"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"idt-worker/internal/model"
	"idt-worker/internal/quality"
	"idt-worker/internal/validate"
)

// TopicRealtime is the 1 Hz sample stream from every S-Box.
const TopicRealtime = "machine/+/realtime"

// Stats are cumulative message counters, logged periodically by main.
type Stats struct {
	Received atomic.Int64
	Written  atomic.Int64
	Rejected atomic.Int64
	Flagged  atomic.Int64
}

// Handler validates each realtime message, counts problems and hands valid samples to Write.
type Handler struct {
	Opts    validate.Options
	Tracker *validate.Tracker
	Quality *quality.Aggregator
	Stats   *Stats
	Write   func(model.MachineData)
	Now     func() time.Time
}

// Handle processes one message. It never blocks on InfluxDB: Write only queues the point.
func (h *Handler) Handle(topic string, payload []byte) {
	now := time.Now()
	if h.Now != nil {
		now = h.Now()
	}
	h.Stats.Received.Add(1)

	res := validate.Realtime(topic, payload, now, h.Opts)
	if res.Reject {
		h.Stats.Rejected.Add(1)
		h.Quality.Add(res.Data.MachineId, res.Issues, now)
		return
	}

	issues := append(res.Issues, h.Tracker.Check(&res.Data)...)
	if len(issues) > 0 {
		h.Stats.Flagged.Add(1)
		h.Quality.Add(res.Data.MachineId, issues, now)
	}
	h.Write(res.Data)
	h.Stats.Written.Add(1)
}

// Subscribe subscribes to machine/+/realtime. With QoS 1 and a persistent
// session the broker queues messages while the worker is down or reconnecting.
func Subscribe(client mqtt.Client, qos byte, h *Handler) error {
	token := client.Subscribe(TopicRealtime, qos, func(_ mqtt.Client, msg mqtt.Message) {
		h.Handle(msg.Topic(), msg.Payload())
	})
	token.Wait()
	if err := token.Error(); err != nil {
		return fmt.Errorf("subscribe %s: %w", TopicRealtime, err)
	}
	log.Printf("mqtt: subscribed to %s QoS=%d", TopicRealtime, qos)
	return nil
}
