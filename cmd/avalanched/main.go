package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/mark-rushakoff/mountainflux/avalanche"
	"github.com/mark-rushakoff/mountainflux/river"
)

var logger = log.New(os.Stdout, "[avalanched] ", log.LstdFlags)

func main() {
	url := flag.String("httpurl", "localhost:8086", "host:port for target HTTP server")
	database := flag.String("database", "", "target database for writes")
	bufSize := flag.Int("bufsize", 65536, "max size of buffer for writes")
	statsURL := flag.String("statsurl", "", "host:port for stats server (to report write throughput)")
	statsDatabase := flag.String("statsdb", "", "database to use on stats server")
	flag.Parse()

	if database == nil || *database == "" {
		logger.Fatalf("no database provided (use e.g. -database=mydb)")
	}

	if statsURL == nil || *statsURL == "" {
		logger.Fatalf("no stats server provided (use e.g. -statsurl=localhost:8086)")
	}

	if statsDatabase == nil || *statsDatabase == "" {
		logger.Fatalf("no stats database provided (use e.g. -statsdb=mydb)")
	}

	c := avalanche.HTTPWriterConfig{
		Host:     "http://" + *url,
		Database: *database,
	}
	w := avalanche.NewHTTPWriter(c)
	logger.Println("Beginning writes to", c.Host)

	statsW := avalanche.NewHTTPWriter(avalanche.HTTPWriterConfig{
		Host:     "http://" + *statsURL,
		Database: *statsDatabase,
	})

	done := make(chan struct{})
	go write(*bufSize, w, statsW, done)

	waitForInterrupt(done)
}

func write(bufSize int, w, statsW avalanche.LineProtocolWriter, done chan struct{}) {
	// Buffer and fields to track write stats
	statsBuf := bytes.NewBuffer(make([]byte, 0, 64*1024))
	statsSeriesKey := river.SeriesKey("avalanched", map[string]string{"pid": fmt.Sprintf("%d", os.Getpid())})
	latField := river.Int{Name: []byte("latNs")}
	successField := river.Bool{Name: []byte("ok")}
	fields := []river.Field{&latField, &successField}

	c := newCounter(bufSize)

	for {
		select {
		case <-done:
			// Write out last set of stats
			statsW.WriteLineProtocol(statsBuf.Bytes())
			return
		default:
			lat, err := w.WriteLineProtocol(c.makeBatch())
			if err != nil {
				logger.Println("write error:", err.Error())
			}

			// Track stats for write
			ts := time.Now().UnixNano()
			latField.Value = lat
			successField.Value = err == nil
			river.WriteLine(statsBuf, statsSeriesKey, fields, ts)

			// Log stats once every 1024 writes
			if (c.ctr & 0x3FF) == 0x3FF {
				statsW.WriteLineProtocol(statsBuf.Bytes())
				statsBuf.Reset()
			}
		}
	}
}

func waitForInterrupt(done chan struct{}) {
	ctrlC := make(chan os.Signal, 1)
	signal.Notify(ctrlC, os.Interrupt)
	for {
		select {
		case <-ctrlC:
			logger.Printf("Interrupted, beginning graceful shutdown...\n")
			close(done)

			os.Exit(0)
		}
	}
}

type counter struct {
	writeBuf  bytes.Buffer
	lineBuf   bytes.Buffer
	lineStart []byte

	ctr int64
}

func newCounter(bufSize int) *counter {
	return &counter{
		writeBuf:  *bytes.NewBuffer(make([]byte, 0, bufSize)),
		lineStart: []byte(fmt.Sprintf("avalanche,pid=%d ctr=", os.Getpid())),
	}
}

func (c *counter) makeBatch() []byte {
	c.writeBuf.Reset()
	if c.lineBuf.Len() > 0 {
		c.writeBuf.Write(c.lineBuf.Bytes())
	}

	for {
		c.lineBuf.Reset()
		c.lineBuf.Write(c.lineStart)
		fmt.Fprintf(&c.lineBuf, "%di %d\n", c.ctr, time.Now().UnixNano())
		c.ctr++

		if c.writeBuf.Len()+c.lineBuf.Len() <= c.writeBuf.Cap() {
			c.writeBuf.Write(c.lineBuf.Bytes())
		} else {
			return c.writeBuf.Bytes()
		}
	}
}
