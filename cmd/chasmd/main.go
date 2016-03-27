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
	logger = log.New(os.Stdout, "[chasmd] ", log.LstdFlags)

	configPath   = flag.String("config", "chasmd.toml", "Path to chasmd configuration file")
	sampleConfig = flag.Bool("sample-config", false, "If set, print out sample configuration and exit")

	bind           = flag.String("httpbind", "0.0.0.0:8086", "TCP bind address for chasm HTTP server")
	statSampleRate = flag.Duration("statSampleRate", 100*time.Millisecond, "Sample rate to capture load stats")
	statServer     = flag.String("statServer", "", "InfluxDB instance to store stats for this server, e.g. example.com:8086")
	statReporters  = flag.Int("statReporters", 4, "Number of workers to report stats")
	statFlushRate  = flag.Duration("statFlushRate", time.Second, "Rate at which to report stats")
	statDb         = flag.String("statDatabase", "chasmd", "Name of database for reported stats")
	statSeriesKey  = flag.String("statSeriesKey", fmt.Sprintf("chasmd,pid=%d,type=http", os.Getpid()), "Series key for each stat point written")

	wg      sync.WaitGroup
	bufPool = sync.Pool{
		New: func() interface{} {
			// Just guessing on the initial buffer size here.
			return bytes.NewBuffer(make([]byte, 0, 1024*16))
		},
	}
	statPayloads chan *bytes.Buffer

	mu  sync.Mutex
	buf = bufPool.Get().(*bytes.Buffer)
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

	var cfg chasmConfig
	if err := toml.NewDecoder(f).Decode(&cfg); err != nil {
		logger.Fatalf(err.Error())
	}
	cfg.Stats.FinalizeSeriesKey()

	validateOptions()

	statPayloads = make(chan *bytes.Buffer, *statReporters)
	sk := []byte(*statSeriesKey)
	bytesAccepted := river.Int{Name: []byte("bytes")}
	linesAccepted := river.Int{Name: []byte("lines")}
	reqsAccepted := river.Int{Name: []byte("reqs")}
	fields := []river.Field{
		&bytesAccepted,
		&linesAccepted,
		&reqsAccepted,
	}

	c := chasm.Config{
		HTTPConfig: &chasm.HTTPConfig{
			Bind: *bind,
		},
	}

	s, err := chasm.NewServer(c)
	if err != nil {
		logger.Fatal("Unexpected error:", err.Error())
	}

	spawnReporters(*statReporters)

	s.Serve()
	logger.Println("HTTP server listening on", s.HTTPURL)

	ctrlC := make(chan os.Signal, 1)
	signal.Notify(ctrlC, os.Interrupt)

	statReportTicker := time.NewTicker(*statFlushRate)
	statSampleTicker := time.NewTicker(*statSampleRate)
	for {
		select {
		case <-ctrlC:
			flushBuffer()
			shutdown(s)
		case <-statReportTicker.C:
			flushBuffer()
		case <-statSampleTicker.C:
			stats := s.HTTPStats()
			bytesAccepted.Value = int64(stats.BytesAccepted)
			linesAccepted.Value = int64(stats.LinesAccepted)
			reqsAccepted.Value = int64(stats.RequestsAccepted)

			mu.Lock()
			// Safe to discard this error because river.WriteLine would only return an error
			// from writing to the io.Writer; and bytes.Buffer does not fail on writes.
			_ = river.WriteLine(buf, sk, fields, time.Now().UnixNano())
			mu.Unlock()
		}
	}
}

func validateOptions() {
	if *statServer == "" {
		logger.Fatalf("Required flag -statServer not provided")
	}
}

func flushBuffer() {
	mu.Lock()
	statPayloads <- buf
	buf = bufPool.Get().(*bytes.Buffer)
	mu.Unlock()
}

func shutdown(s *chasm.Server) {
	logger.Printf("Interrupted, beginning graceful shutdown...\n")

	done := make(chan struct{})

	go func() {
		s.Close()
		close(statPayloads)
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

func spawnReporters(n int) {
	c := avalanche.HTTPWriterConfig{
		Host:     "http://" + *statServer,
		Database: *statDb,
	}
	wg.Add(n)
	for i := 0; i < n; i++ {
		w := avalanche.NewHTTPWriter(c)
		go report(w)
	}
}

func report(w avalanche.LineProtocolWriter) {
	for buf := range statPayloads {
		if _, err := w.WriteLineProtocol(buf.Bytes()); err != nil {
			logger.Printf("Error writing stats: %s", err.Error())
		}
		bufPool.Put(buf)
	}

	wg.Done()
}
