package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"
	"idt-worker/internal/influx"
	"idt-worker/internal/model"
	"idt-worker/internal/mqtt"
)

func main() {
	// MQTT
	mqttBroker := getEnv("MQTT_BROKER", "tcp://localhost:1883")
	mqttClientIDBase := getEnv("MQTT_CLIENT_ID", "idt-worker")
	mqttClientID := mqttClientIDWithSuffix(mqttClientIDBase)
	mqttUser := getEnv("MQTT_USER", "")
	mqttPass := getEnv("MQTT_PASS", "")

	// InfluxDB
	influxURL := getEnv("INFLUX_URL", "http://localhost:8086")
	influxToken := getEnv("INFLUX_TOKEN", "")
	influxOrg := getEnv("INFLUX_ORG", "my-org")
	influxBucket := getEnv("INFLUX_BUCKET", "machine")

	writer, err := influx.NewWriter(influxURL, influxToken, influxOrg, influxBucket)
	if err != nil {
		log.Fatalf("influx: %v", err)
	}
	defer writer.Close()

	ctx := context.Background()
	if err := writer.Health(ctx); err != nil {
		log.Fatalf("influx health check failed (URL=%s org=%s bucket=%s): %v", influxURL, influxOrg, influxBucket, err)
	}
	log.Printf("influx: connected OK (org=%s bucket=%s)", influxOrg, influxBucket)

	var onMessage func(model.MachineData) error
	onMessage = func(d model.MachineData) error {
		return writer.Write(context.Background(), d)
	}

	opts := pahomqtt.NewClientOptions().
		AddBroker(mqttBroker).
		SetClientID(mqttClientID).
		SetAutoReconnect(true).
		SetConnectRetry(true).
		SetConnectRetryInterval(10 * time.Second).
		SetConnectionLostHandler(func(_ pahomqtt.Client, err error) {
			log.Printf("mqtt: connection lost: %v (reconnecting...)", err)
		}).
		SetOnConnectHandler(func(c pahomqtt.Client) {
			log.Printf("mqtt: connected (client_id=%s)", mqttClientID)
			if err := mqtt.Subscribe(c, onMessage); err != nil {
				log.Printf("mqtt: subscribe error: %v", err)
			}
		})
	if mqttUser != "" {
		opts.SetUsername(mqttUser).SetPassword(mqttPass)
	}

	client := pahomqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Fatalf("mqtt connect: %v", token.Error())
	}
	defer client.Disconnect(250)

	log.Println("worker running; subscribe machine/+/realtime -> InfluxDB")
	waitSignal()
}

// mqttClientIDWithSuffix appends a short random hex suffix to avoid duplicate client IDs.
func mqttClientIDWithSuffix(base string) string {
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

func waitSignal() {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Println("shutting down")
}
