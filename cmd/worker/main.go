package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"

	"idt-worker/internal/influx"
	"idt-worker/internal/mqtt"
	"idt-worker/internal/quality"
	"idt-worker/internal/validate"
)

func main() {
	// MQTT
	mqttBroker := getEnv("MQTT_BROKER", "tcp://localhost:1883")
	mqttClientID := getEnv("MQTT_CLIENT_ID", "idt-worker")
	if getBool("MQTT_CLIENT_ID_RANDOM_SUFFIX", false) {
		// only for running several throwaway workers — a random id cannot resume its session
		mqttClientID = withRandomSuffix(mqttClientID)
	}
	mqttUser := getEnv("MQTT_USER", "")
	mqttPass := getEnv("MQTT_PASS", "")
	mqttQoS := byte(getInt("MQTT_QOS", 1))
	cleanSession := getBool("MQTT_CLEAN_SESSION", false)

	// InfluxDB
	influxURL := getEnv("INFLUX_URL", "http://localhost:8086")
	influxToken := getEnv("INFLUX_TOKEN", "")
	influxOrg := getEnv("INFLUX_ORG", "my-org")
	influxBucket := getEnv("INFLUX_BUCKET", "machine")

	opts := validate.Options{
		TimestampPolicy: getEnv("TIMESTAMP_POLICY", validate.TimestampPolicyServer),
		FutureSkew:      time.Duration(getInt("INGEST_FUTURE_SKEW_SECONDS", 120)) * time.Second,
		LateAfter:       time.Duration(getInt("INGEST_LATE_SECONDS", 60)) * time.Second,
	}
	statsEvery := time.Duration(getInt("STATS_INTERVAL_SECONDS", 60)) * time.Second

	writer := influx.NewWriter(influxURL, influxToken, influxOrg, influxBucket, influx.Options{})
	if err := writer.Health(context.Background()); err != nil {
		log.Fatalf("influx health check failed (URL=%s org=%s bucket=%s): %v", influxURL, influxOrg, influxBucket, err)
	}
	log.Printf("influx: connected OK (org=%s bucket=%s)", influxOrg, influxBucket)

	stats := &mqtt.Stats{}
	agg := quality.NewAggregator()
	handler := &mqtt.Handler{
		Opts:    opts,
		Tracker: validate.NewTracker(),
		Quality: agg,
		Stats:   stats,
		Write:   writer.WriteSample,
	}

	clientOpts := pahomqtt.NewClientOptions().
		AddBroker(mqttBroker).
		SetClientID(mqttClientID).
		SetCleanSession(cleanSession).
		SetResumeSubs(true).
		SetAutoReconnect(true).
		SetConnectRetry(true).
		SetConnectRetryInterval(10 * time.Second).
		SetConnectionLostHandler(func(_ pahomqtt.Client, err error) {
			log.Printf("mqtt: connection lost: %v (reconnecting...)", err)
		}).
		SetOnConnectHandler(func(c pahomqtt.Client) {
			log.Printf("mqtt: connected (client_id=%s clean_session=%t)", mqttClientID, cleanSession)
			if err := mqtt.Subscribe(c, mqttQoS, handler); err != nil {
				log.Printf("mqtt: subscribe error: %v", err)
			}
		})
	if mqttUser != "" {
		clientOpts.SetUsername(mqttUser).SetPassword(mqttPass)
	}

	client := pahomqtt.NewClient(clientOpts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Fatalf("mqtt connect: %v", token.Error())
	}

	stop := make(chan struct{})
	go flushQuality(agg, writer, stop)
	go logStats(stats, writer, statsEvery, stop)

	log.Printf("worker running; subscribe %s QoS=%d -> InfluxDB", mqtt.TopicRealtime, mqttQoS)
	waitSignal()

	close(stop)
	client.Disconnect(250)
	writer.WriteQuality(agg.Drain(time.Time{}))
	writer.Close()
	printStats(stats, writer)
	log.Println("worker stopped")
}

// flushQuality writes finished minutes of reject/flag counters.
func flushQuality(agg *quality.Aggregator, w *influx.Writer, stop <-chan struct{}) {
	t := time.NewTicker(15 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case now := <-t.C:
			w.WriteQuality(agg.Drain(now.UTC().Truncate(time.Minute)))
		}
	}
}

func logStats(s *mqtt.Stats, w *influx.Writer, every time.Duration, stop <-chan struct{}) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			printStats(s, w)
		}
	}
}

func printStats(s *mqtt.Stats, w *influx.Writer) {
	line, _ := json.Marshal(map[string]any{
		"level":       "info",
		"message":     "ingest stats",
		"timestamp":   time.Now().UTC().Format(time.RFC3339),
		"received":    s.Received.Load(),
		"written":     s.Written.Load(),
		"rejected":    s.Rejected.Load(),
		"flagged":     s.Flagged.Load(),
		"writeErrors": w.WriteErrors(),
	})
	log.Println(string(line))
}

func withRandomSuffix(base string) string {
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%s-%d", base, time.Now().UnixNano()%100000)
	}
	return fmt.Sprintf("%s-%s", base, hex.EncodeToString(b))
}

func getEnv(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

func getInt(key string, defaultVal int) int {
	v := os.Getenv(key)
	if v == "" {
		return defaultVal
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		log.Printf("config: %s=%q is not a number, using %d", key, v, defaultVal)
		return defaultVal
	}
	return n
}

func getBool(key string, defaultVal bool) bool {
	switch strings.ToLower(os.Getenv(key)) {
	case "1", "true", "yes":
		return true
	case "0", "false", "no":
		return false
	default:
		return defaultVal
	}
}

func waitSignal() {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Println("shutting down")
}
