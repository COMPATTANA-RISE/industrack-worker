package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"
	"idt-worker/internal/influx"
	"idt-worker/internal/model"
	"idt-worker/internal/mqtt"
)

func main() {
	// MQTT
	mqttBroker := getEnv("MQTT_BROKER", "tcp://localhost:1883")
	mqttClientID := getEnv("MQTT_CLIENT_ID", "idt-worker")
	mqttUser := getEnv("MQTT_USER", "")
	mqttPass := getEnv("MQTT_PASS", "")

	// InfluxDB
	influxURL := getEnv("INFLUX_URL", "http://localhost:8086")
	influxToken := getEnv("INFLUX_TOKEN", "")
	influxOrg := getEnv("INFLUX_ORG", "my-org")
	influxBucket := getEnv("INFLUX_BUCKET", "machine")

	opts := pahomqtt.NewClientOptions().
		AddBroker(mqttBroker).
		SetClientID(mqttClientID)
	if mqttUser != "" {
		opts.SetUsername(mqttUser).SetPassword(mqttPass)
	}

	client := pahomqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Fatalf("mqtt connect: %v", token.Error())
	}
	defer client.Disconnect(250)

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

	onMessage := func(d model.MachineData) error {
		return writer.Write(context.Background(), d)
	}
	if err := mqtt.Subscribe(client, onMessage); err != nil {
		log.Fatalf("subscribe: %v", err)
	}

	log.Println("worker running; subscribe machine/+/realtime -> InfluxDB")
	waitSignal()
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
