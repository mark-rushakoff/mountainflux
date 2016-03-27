package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/influxdata/toml"
	"github.com/mark-rushakoff/mountainflux/avalanche"
	"github.com/mark-rushakoff/mountainflux/chasm"
	"github.com/mark-rushakoff/mountainflux/river"
)

var (
	cfg chasmConfig

	logger = log.New(os.Stdout, "[chasmd] ", log.LstdFlags)

	configPath   = flag.String("config", "chasmd.toml", "Path to chasmd configuration file")
	sampleConfig = flag.Bool("sample-config", false, "If set, print out sample configuration and exit")

	wg      sync.WaitGroup
	bufPool = sync.Pool{
		New: func() interface{} {
			// Just guessing on the initial buffer size here.
			return bytes.NewBuffer(make([]byte, 0, 1024*16))
		},
	}
	statPayloads chan *bytes.Buffer
)

func main() {
	flag.Parse()

	if *sampleConfig {
		fmt.Println(sampleConfigText)
		os.Exit(0)
	}

	f, err := os.Open(*configPath)
	if err != nil {
		logger.Fatalf(err.Error())
	}

	if err := toml.NewDecoder(f).Decode(&cfg); err != nil {
		logger.Fatalf(err.Error())
	}
	cfg.Stats.FinalizeSeriesKey()
	logger.Println("Recording stats with series key:", cfg.Stats.SeriesKey)

	if cfg.Stats.NumWorkers <= 0 {
		logger.Fatalf("stats.workers must be > 0")
	}

	statPayloads = make(chan *bytes.Buffer, cfg.Stats.NumWorkers)

	c := chasm.Config{
		HTTPConfig: &chasm.HTTPConfig{
			Bind: cfg.HTTP.Bind,
		},
	}

	s, serverStats, err := chasm.NewServer(c)
	if err != nil {
		logger.Fatal("Unexpected error:", err.Error())
	}

	wg.Add(1)
	go collectServerStats(serverStats)

	spawnStatWorkers()

	s.Serve()
	logger.Println("HTTP server listening on", s.HTTPURL)

	ctrlC := make(chan os.Signal, 1)
	signal.Notify(ctrlC, os.Interrupt)

	select {
	case <-ctrlC:
		shutdown(s)
	}
}

// collectServerStats is intended to be run in a single goroutine.
// Collects stats from the server into batches and sends off to stat reporters.
func collectServerStats(serverStats <-chan chasm.Stats) {
	buf := bufPool.Get().(*bytes.Buffer)
	curLines := 0
	maxLines := cfg.Stats.BatchSize

	sk := []byte(cfg.Stats.SeriesKey)
	bytesAccepted := river.Int{Name: []byte("bytes")}
	ingestLatency := river.Int{Name: []byte("ingestLatNs")}
	linesAccepted := river.Int{Name: []byte("lines")}
	fields := []river.Field{
		&bytesAccepted,
		&ingestLatency,
		&linesAccepted,
	}

	for stats := range serverStats {
		bytesAccepted.Value = int64(stats.BytesAccepted)
		ingestLatency.Value = int64(stats.IngestLatency)
		linesAccepted.Value = int64(stats.LinesAccepted)

		// Safe to discard this error because river.WriteLine would only return an error
		// from writing to the io.Writer; and bytes.Buffer does not fail on writes.
		_ = river.WriteLine(buf, sk, fields, stats.Time)

		curLines++
		if curLines >= maxLines {
			statPayloads <- buf
			curLines = 0
			buf = bufPool.Get().(*bytes.Buffer)
		}
	}

	// serverStats was closed. May need one last flush.
	if curLines > 0 {
		statPayloads <- buf
	}

	close(statPayloads)

	wg.Done()
}

func shutdown(s *chasm.Server) {
	logger.Printf("Interrupted, beginning graceful shutdown...\n")

	done := make(chan struct{})

	go func() {
		s.Close()
		wg.Wait()
		done <- struct{}{}
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		logger.Printf("Graceful shutdown not completed in time. Aborting...")
	}

	os.Exit(0)
}

func spawnStatWorkers() {
	c := avalanche.HTTPWriterConfig{
		Host:     cfg.Stats.Host,
		Database: cfg.Stats.Database,
	}
	wg.Add(cfg.Stats.NumWorkers)
	for i := 0; i < cfg.Stats.NumWorkers; i++ {
		w := avalanche.NewHTTPWriter(c)
		go report(w)
	}
}

func report(w avalanche.LineProtocolWriter) {
	for buf := range statPayloads {
		if _, err := w.WriteLineProtocol(buf.Bytes()); err != nil {
			logger.Printf("Error writing stats: %s", err.Error())
		}

		buf.Reset()
		bufPool.Put(buf)
	}

	wg.Done()
}
