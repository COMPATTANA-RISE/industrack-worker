// Command simulator publishes realistic S-Box messages (start → realtime → data,
// plus status) to an MQTT broker so the whole INDUSTRACK pipeline can be tested
// without a real machine. At the end it prints the totals the server should
// compute, so completeness, stroke and energy figures can be checked exactly.
//
// Payload format: docs/DEVICE-PAYLOAD-CONTRACT.md (current firmware: "pf" carries Ptot kW).
//
// Examples:
//
//	go run ./cmd/simulator --devices ABC123DEF456 --job SB6902258#AB12 --duration 30m --fast
//	go run ./cmd/simulator --scenario counter-reset --drop-rate 0.01 --dup-rate 0.02 --fast
//	go run ./cmd/simulator --scenario stops --fast
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"sort"
	"strings"
	"time"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"
)

var scenarios = []string{"golden", "gaps", "offline", "counter-reset", "zero-glitch", "invalid", "clock-skew", "late-burst", "stops"}

type config struct {
	broker, user, pass string
	devices            []string
	job                string
	person             string
	manPower           int
	scenario           string
	rate               time.Duration
	duration           time.Duration
	maxGap             time.Duration
	qos                byte
	seed               int64
	dropRate, dupRate  float64
	fast               bool
	startStroke        int64
	startWh            float64
	noStart, noEnd     bool
	dryRun             bool
}

// segment of the machine timeline.
type segment struct {
	kind string // setup | work | idle | offline(work while the network is down)
	dur  time.Duration
}

type message struct {
	topic   string
	payload []byte
	retain  bool
}

// totals is what the server should report for one device.
type totals struct {
	Device                 string  `json:"device"`
	Scenario               string  `json:"scenario"`
	From                   string  `json:"from"`
	To                     string  `json:"to"`
	SamplesGenerated       int     `json:"samplesGenerated"`
	SamplesPublished       int     `json:"samplesPublished"`
	SamplesDropped         int     `json:"samplesDropped"`
	SamplesDuplicated      int     `json:"samplesDuplicated"`
	SamplesCorrupted       int     `json:"samplesCorrupted"`
	SamplesBadJSON         int     `json:"samplesBadJson"`
	OfflineSeconds         float64 `json:"offlineSeconds"`
	StrokesTrue            int64   `json:"strokesTrue"`
	WhTrue                 float64 `json:"whTrue"`
	WhWorkingTrue          float64 `json:"whWorkingTrue"`
	WhIdleTrue             float64 `json:"whIdleTrue"`
	WorkingSecondsTrue     float64 `json:"workingSecondsTrue"`
	PoweredSecondsMeasured float64 `json:"poweredSecondsMeasured"`
	WorkingSecondsMeasured float64 `json:"workingSecondsMeasured"`
	CounterResets          int     `json:"counterResets"`
	Stops                  []stop  `json:"stops,omitempty"`
}

type stop struct {
	Kind  string `json:"kind"`
	Start string `json:"start"`
	End   string `json:"end"`
	Sec   int    `json:"seconds"`
}

func main() {
	cfg := parseFlags()

	var client pahomqtt.Client
	if !cfg.dryRun {
		client = connect(cfg)
		defer client.Disconnect(500)
	}

	start := time.Now().Truncate(time.Second)
	if cfg.fast {
		// replay a past window, ending now, as fast as the broker accepts it
		start = start.Add(-timelineDuration(cfg))
	}

	var all []totals
	for i, dev := range cfg.devices {
		devRng := rand.New(rand.NewSource(cfg.seed + int64(i)*7919))
		t := simulate(cfg, dev, start, devRng, func(m message) { publish(client, cfg, m) })
		all = append(all, t)
	}
	report(all)
}

func parseFlags() config {
	var c config
	var devices, qos string
	flag.StringVar(&c.broker, "broker", env("MQTT_BROKER", "tcp://localhost:1883"), "MQTT broker URL")
	flag.StringVar(&c.user, "user", env("MQTT_USER", ""), "MQTT username")
	flag.StringVar(&c.pass, "pass", env("MQTT_PASS", ""), "MQTT password")
	flag.StringVar(&devices, "devices", "SIMDEVICE001", "comma-separated Machine.deviceId values (topic segment)")
	flag.StringVar(&c.job, "job", "SB6902258#SIM1", "jobId sent by the device: JOBCODE#COMPANYCODE")
	flag.StringVar(&c.person, "person", "simulator", "operator name")
	flag.IntVar(&c.manPower, "man-power", 2, "people at the machine")
	flag.StringVar(&c.scenario, "scenario", "golden", "one of: "+strings.Join(scenarios, ", "))
	flag.DurationVar(&c.rate, "rate", time.Second, "sample interval")
	flag.DurationVar(&c.duration, "duration", 10*time.Minute, "job length (ignored by --scenario stops)")
	flag.DurationVar(&c.maxGap, "max-gap", 10*time.Second, "server MACHINE_TIME_MAX_GAP_SECONDS used for the expected measured times")
	flag.StringVar(&qos, "qos", "1", "publish QoS (0 or 1)")
	flag.Int64Var(&c.seed, "seed", 42, "random seed (same seed = same data)")
	flag.Float64Var(&c.dropRate, "drop-rate", 0, "probability a realtime message is not published")
	flag.Float64Var(&c.dupRate, "dup-rate", 0, "probability a realtime message is published twice")
	flag.BoolVar(&c.fast, "fast", false, "replay the window ending now without sleeping")
	flag.Int64Var(&c.startStroke, "start-stroke", 1000, "stroke counter at job start")
	flag.Float64Var(&c.startWh, "start-wh", 500000, "energy counter (Wh) at job start")
	flag.BoolVar(&c.noStart, "no-start", false, "do not publish machine/{id}/start")
	flag.BoolVar(&c.noEnd, "no-end", false, "do not publish the machine/{id}/data summary")
	flag.BoolVar(&c.dryRun, "dry-run", false, "print totals without connecting to a broker")
	flag.Parse()

	for _, d := range strings.Split(devices, ",") {
		if d = strings.TrimSpace(d); d != "" {
			c.devices = append(c.devices, d)
		}
	}
	if len(c.devices) == 0 {
		log.Fatal("--devices is empty")
	}
	if c.duration < 3*time.Minute {
		log.Fatal("--duration must be at least 3m")
	}
	if !contains(scenarios, c.scenario) {
		log.Fatalf("unknown --scenario %q (use: %s)", c.scenario, strings.Join(scenarios, ", "))
	}
	c.qos = 1
	if qos == "0" {
		c.qos = 0
	}
	if c.fast && (c.scenario == "clock-skew" || c.scenario == "late-burst") {
		log.Printf("note: --scenario %s only shows its effect in real time (without --fast)", c.scenario)
	}
	return c
}

func connect(cfg config) pahomqtt.Client {
	opts := pahomqtt.NewClientOptions().
		AddBroker(cfg.broker).
		SetClientID(fmt.Sprintf("industrack-simulator-%d", time.Now().UnixNano()%1_000_000)).
		SetCleanSession(true).
		SetConnectTimeout(10 * time.Second)
	if cfg.user != "" {
		opts.SetUsername(cfg.user).SetPassword(cfg.pass)
	}
	if len(cfg.devices) == 1 {
		// what the real S-Box should do: broker publishes this if we vanish
		opts.SetWill("machine/"+cfg.devices[0]+"/status", `{"status":false}`, 1, true)
	}
	c := pahomqtt.NewClient(opts)
	if tok := c.Connect(); tok.Wait() && tok.Error() != nil {
		log.Fatalf("mqtt connect %s: %v", cfg.broker, tok.Error())
	}
	return c
}

func publish(c pahomqtt.Client, cfg config, m message) {
	if c == nil {
		return
	}
	tok := c.Publish(m.topic, cfg.qos, m.retain, m.payload)
	if cfg.qos > 0 || cfg.fast {
		tok.Wait()
	}
	if err := tok.Error(); err != nil {
		log.Printf("publish %s: %v", m.topic, err)
	}
}

// timeline builds the machine segments for a scenario.
func timeline(cfg config, rng *rand.Rand) []segment {
	if cfg.scenario == "stops" {
		return []segment{
			{"setup", 8 * time.Minute},
			{"work", 20 * time.Minute},
			{"idle", 10 * time.Minute},
			{"work", 10 * time.Minute},
			{"offline", 5 * time.Minute},
			{"work", 10 * time.Minute},
		}
	}
	segs := []segment{{"setup", randDur(rng, 30, 60)}}
	total := segs[0].dur
	for total < cfg.duration {
		for _, kind := range []string{"work", "idle"} {
			d := randDur(rng, 60, 180)
			if kind == "idle" {
				d = randDur(rng, 15, 90)
			}
			if total+d > cfg.duration {
				d = cfg.duration - total
			}
			if d > 0 {
				segs = append(segs, segment{kind, d})
				total += d
			}
		}
	}
	if cfg.scenario == "offline" {
		// network down for 5 minutes at 40% of the job; the machine keeps working
		segs = splitAt(segs, time.Duration(float64(cfg.duration)*0.4), 5*time.Minute)
	}
	return segs
}

// timelineDuration is the same for every device: random segments are cut to --duration.
func timelineDuration(cfg config) time.Duration {
	switch cfg.scenario {
	case "stops":
		return 63 * time.Minute
	case "offline":
		return cfg.duration + 5*time.Minute
	}
	return cfg.duration
}

// splitAt inserts an offline segment of length off at time at (machine keeps working).
func splitAt(segs []segment, at, off time.Duration) []segment {
	var out []segment
	var t time.Duration
	inserted := false
	for _, s := range segs {
		if !inserted && t+s.dur > at {
			before := at - t
			if before > 0 {
				out = append(out, segment{s.kind, before})
			}
			out = append(out, segment{"offline", off})
			if rest := s.dur - before; rest > 0 {
				out = append(out, segment{s.kind, rest})
			}
			inserted = true
		} else {
			out = append(out, s)
		}
		t += s.dur
	}
	return out
}

type sample struct {
	ts      time.Time
	working bool
}

func simulate(cfg config, dev string, start time.Time, rng *rand.Rand, send func(message)) totals {
	segs := timeline(cfg, rng)
	tot := totals{Device: dev, Scenario: cfg.scenario, From: start.UTC().Format(time.RFC3339)}

	stroke := cfg.startStroke
	wh := cfg.startWh
	runSec := 0.0
	var seq int64
	kw := 1.1
	now := start
	dt := cfg.rate.Seconds()
	clockSkew := time.Duration(0)
	if cfg.scenario == "clock-skew" {
		clockSkew = 5 * time.Minute
	}
	prevWorking := false   // state of the previous sample (energy attribution)
	var buffered []message // late-burst
	burstFrom := start.Add(time.Duration(float64(sumDur(segs)) * 0.3))
	burstTo := burstFrom.Add(2 * time.Minute)
	resetAt := start.Add(sumDur(segs) / 2)
	didReset := false
	var published []sample
	gapLeft := 0

	status := func(online bool) {
		send(message{"machine/" + dev + "/status", mustJSON(map[string]any{"status": online, "timestamp": now.Add(clockSkew).UnixMilli()}), true})
	}
	status(true)
	if !cfg.noStart {
		send(message{"machine/" + dev + "/start", mustJSON(map[string]any{
			"timestamp": now.Add(clockSkew).UnixMilli(), "jobId": cfg.job, "person": cfg.person,
			"manPower": cfg.manPower, "serial": "SIM-" + dev, "stroke": stroke, "wh": round2(wh),
		}), false})
	}

	for _, seg := range segs {
		segStart := now
		steps := int(math.Round(seg.dur.Seconds() / dt))
		for i := 0; i < steps; i++ {
			next := now.Add(cfg.rate)
			advance := func() {
				if !cfg.fast {
					time.Sleep(time.Until(next)) // absolute schedule: no drift over long runs
				}
				now = next
			}
			working := seg.kind == "work" || seg.kind == "offline"
			if cfg.scenario == "stops" && seg.kind == "offline" {
				working = false // scripted stop: the machine is off-line and not producing
			}
			if cfg.scenario == "counter-reset" && !didReset && !now.Before(resetAt) {
				stroke = 0 // device-side reset; strokes keep counting from 0
				didReset = true
				tot.CounterResets++
			}
			// machine physics for this step
			if working {
				kw = clamp(kw+rng.NormFloat64()*0.3, 4.0, 7.5)
				if rng.Float64() < dt/4.5 {
					stroke++
					tot.StrokesTrue++
				}
				runSec += dt
				tot.WorkingSecondsTrue += dt
			} else {
				kw = clamp(1.1+rng.NormFloat64()*0.05, 0.9, 1.3)
			}
			dWh := kw * dt / 3600 * 1000
			wh += dWh
			tot.WhTrue += dWh
			// The server attributes an increment to the state of the PREVIOUS sample
			// (a sample's state holds until the next one), so split the same way here.
			if prevWorking {
				tot.WhWorkingTrue += dWh
			} else {
				tot.WhIdleTrue += dWh
			}
			prevWorking = working

			if seg.kind == "offline" {
				tot.OfflineSeconds += dt
				advance()
				continue
			}
			tot.SamplesGenerated++
			seq++

			drop := rng.Float64() < cfg.dropRate
			if cfg.scenario == "gaps" {
				if gapLeft == 0 && rng.Float64() < 0.01 {
					gapLeft = 2 + rng.Intn(7)
				}
				if gapLeft > 0 {
					gapLeft--
					drop = true
				}
			}
			if drop {
				tot.SamplesDropped++
				advance()
				continue
			}

			volt := 380 + rng.NormFloat64()*2
			truePF := 0.2
			if working {
				truePF = 0.8
			}
			amp := kw * 1000 / (math.Sqrt(3) * volt * truePF)
			p := map[string]any{
				"timestamp": now.Add(clockSkew).UnixMilli(),
				"jobId":     cfg.job,
				"working":   working,
				"status":    working,
				"stroke":    stroke,
				"wh":        round2(wh),
				"volt":      round2(volt),
				"amp":       round2(amp),
				"pf":        round2(kw), // current firmware: Ptot (kW) in "pf"
				"time":      int64(runSec),
				"person":    cfg.person,
				"manPower":  cfg.manPower,
				"serial":    "SIM-" + dev,
				"seq":       seq,
			}
			payload := mustJSON(p)
			switch cfg.scenario {
			case "zero-glitch":
				if rng.Float64() < 0.01 {
					p["stroke"], p["wh"] = 0, 0
					payload = mustJSON(p)
					tot.SamplesCorrupted++
				}
			case "invalid":
				if rng.Float64() < 0.02 {
					switch rng.Intn(4) {
					case 0:
						p["stroke"] = "abc"
					case 1:
						p["wh"] = "NaN"
					case 2:
						p["volt"] = -1
					case 3:
						p = nil // truncated JSON → the whole message is rejected
					}
					if p == nil {
						payload = payload[:len(payload)/2]
						tot.SamplesBadJSON++
					} else {
						payload = mustJSON(p)
						tot.SamplesCorrupted++
					}
				}
			}

			m := message{"machine/" + dev + "/realtime", payload, false}
			if cfg.scenario == "late-burst" && !cfg.fast && !now.Before(burstFrom) && now.Before(burstTo) {
				buffered = append(buffered, m) // store-and-forward: sent after the outage
			} else {
				if len(buffered) > 0 {
					for _, b := range buffered {
						send(b)
					}
					buffered = nil
				}
				send(m)
			}
			tot.SamplesPublished++
			if isValidJSON(payload) {
				published = append(published, sample{now, working})
			}
			if rng.Float64() < cfg.dupRate {
				send(m)
				tot.SamplesDuplicated++
			}
			advance()
		}
		if seg.kind != "work" && (cfg.scenario == "stops" || seg.kind == "offline") {
			tot.Stops = append(tot.Stops, stop{seg.kind, segStart.UTC().Format(time.RFC3339), now.UTC().Format(time.RFC3339), int(now.Sub(segStart).Seconds())})
		}
	}
	for _, b := range buffered {
		send(b)
	}

	if !cfg.noEnd {
		send(message{"machine/" + dev + "/data", mustJSON(map[string]any{
			"timestamp": now.Add(clockSkew).UnixMilli(), "jobId": cfg.job, "person": cfg.person,
			"manPower": cfg.manPower, "serial": "SIM-" + dev, "stroke": stroke, "wh": round2(wh),
			"time": int64(runSec), "volt": 380.0, "amp": 2.0, "pf": 1.1, "status": false,
		}), false})
	}
	status(false)

	tot.To = now.UTC().Format(time.RFC3339)
	tot.PoweredSecondsMeasured, tot.WorkingSecondsMeasured = measured(published, cfg.maxGap)
	return tot
}

// measured reproduces the server's dwell rule: each gap counts up to maxGap and
// belongs to the state of the sample before it. The last sample is closed the
// same way the server closes it once it knows no sample followed (dataEnd),
// so these totals can be compared with the API exactly.
func measured(s []sample, maxGap time.Duration) (powered, working float64) {
	sort.SliceStable(s, func(i, j int) bool { return s[i].ts.Before(s[j].ts) })
	for i := 0; i < len(s); i++ {
		var gap time.Duration
		if i+1 < len(s) {
			gap = s[i+1].ts.Sub(s[i].ts)
			if gap <= 0 {
				continue
			}
		} else {
			gap = maxGap // no next sample: the server counts up to maxGap, then calls it offline
		}
		if gap > maxGap {
			gap = maxGap
		}
		powered += gap.Seconds()
		if s[i].working {
			working += gap.Seconds()
		}
	}
	return powered, working
}

func report(all []totals) {
	for _, t := range all {
		fmt.Printf("\n== %s (%s) %s → %s\n", t.Device, t.Scenario, t.From, t.To)
		fmt.Printf("samples: generated %d, published %d, dropped %d, duplicated %d, corrupted %d (bad JSON %d)\n",
			t.SamplesGenerated, t.SamplesPublished, t.SamplesDropped, t.SamplesDuplicated, t.SamplesCorrupted, t.SamplesBadJSON)
		fmt.Printf("strokes: %d  energy: %.2f Wh (working %.2f, idle %.2f)  counter resets: %d\n",
			t.StrokesTrue, t.WhTrue, t.WhWorkingTrue, t.WhIdleTrue, t.CounterResets)
		fmt.Printf("time: working (true) %.0fs · measured powered %.0fs, working %.0fs · offline %.0fs\n",
			t.WorkingSecondsTrue, t.PoweredSecondsMeasured, t.WorkingSecondsMeasured, t.OfflineSeconds)
		for _, s := range t.Stops {
			fmt.Printf("stop: %-7s %s → %s (%ds)\n", s.Kind, s.Start, s.End, s.Sec)
		}
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	fmt.Println("\n--- totals (JSON) ---")
	_ = enc.Encode(all)
}

func randDur(rng *rand.Rand, minSec, maxSec int) time.Duration {
	return time.Duration(minSec+rng.Intn(maxSec-minSec+1)) * time.Second
}

func sumDur(segs []segment) time.Duration {
	var d time.Duration
	for _, s := range segs {
		d += s.dur
	}
	return d
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		log.Fatal(err)
	}
	return b
}

func isValidJSON(b []byte) bool { return json.Valid(b) }

func round2(v float64) float64 { return math.Round(v*100) / 100 }

func clamp(v, lo, hi float64) float64 { return math.Max(lo, math.Min(hi, v)) }

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
