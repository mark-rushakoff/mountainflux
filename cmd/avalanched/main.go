package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"time"

	"github.com/mark-rushakoff/mountainflux/avalanche"
	"github.com/mark-rushakoff/mountainflux/river"
)

var (
	logger = log.New(os.Stdout, "[avalanched] ", log.LstdFlags)

	// One WaitGroup for the HTTP writers
	workersWg sync.WaitGroup

	// Separate WaitGroup for the stats writer (so that it can flush the last set of stats when workers shut down)
	statsWg sync.WaitGroup

	bufPool = sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer(make([]byte, 0, 4*1024))
		},
	}

	// Input scanner sends buffers over this channel, writer workers read from it.
	// Created at runtime with buffer size equivalent to number of workers.
	batchChan chan *bytes.Buffer

	// Channel closed by input scanner when input EOF reached.
	// Indicates to rest of application that it is time to shut down.
	inputDone = make(chan struct{})

	// Channel closed by shutdown function.
	// Indicates to stat flushing function to do one last flush.
	quit = make(chan struct{})

	statMu  sync.Mutex
	statBuf *bytes.Buffer = bufPool.Get().(*bytes.Buffer)
)

func main() {
	url := flag.String("httpurl", "localhost:8086", "host:port for target HTTP server")
	database := flag.String("database", "", "target database for writes")

	linesPerBatch := flag.Int("linesPerBatch", 100, "How many lines to collect before initiating a write")
	numWorkers := flag.Int("workers", 8*runtime.GOMAXPROCS(0), "Number of workers to concurrently send requests to target server")

	statsURL := flag.String("statsurl", "", "host:port for stats server (to report write throughput)")
	statsDatabase := flag.String("statsdb", "", "database to use on stats server")

	// TODO: statsKey ought to include the target url.
	defaultStatsKey := string(river.SeriesKey("avalanched", map[string]string{"pid": fmt.Sprintf("%d", os.Getpid())}))
	statsKey := flag.String("statskey", defaultStatsKey, "Series key to use to report stats")
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

	batchChan = make(chan *bytes.Buffer, *numWorkers)

	// One goroutine to periodically flush stats.
	statsWg.Add(1)
	go recordStats(avalanche.NewHTTPWriter(avalanche.HTTPWriterConfig{
		Host:     "http://" + *statsURL,
		Database: *statsDatabase,
	}))

	// Start the requested number of workers to make write requests over HTTP.
	c := avalanche.HTTPWriterConfig{
		Host:     "http://" + *url,
		Database: *database,
	}
	workersWg.Add(*numWorkers)
	for i := 0; i < *numWorkers; i++ {
		w := avalanche.NewHTTPWriter(c)
		go processBatches([]byte(*statsKey), w, batchChan)
	}
	logger.Println("Beginning writes to", c.Host)

	// Read input on a separate goroutine.
	// Synchronization unnecessary here - if workers are stopped, input scanner will eventually fill the batchChan and block.
	go scan(*linesPerBatch)

	// Wait for either ctrl-c or end of input
	waitForInterrupt()
}

// scan reads one line at a time from stdin.
// When the requested number of lines per batch is met, send a batch over batchChan for the workers to write.
func scan(linesPerBatch int) {
	buf := bufPool.Get().(*bytes.Buffer)

	var n int
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		// *Probably* safe to append to scanner.Bytes this way, to avoid an extra call to buf.WriteByte('\n').
		b := append(scanner.Bytes(), '\n')
		buf.Write(b)

		n++
		if n >= linesPerBatch {
			batchChan <- buf
			buf = bufPool.Get().(*bytes.Buffer)
			n = 0
		}
	}

	if err := scanner.Err(); err != nil {
		logger.Fatalf("Error reading input: %s", err.Error())
	}

	// Finished reading input, make sure last batch goes out.
	if n > 0 {
		batchChan <- buf
	}

	// Closing inputDone signals to the application that we've read everything and can now shut down.
	close(inputDone)
}

// processBatches reads byte buffers from batchChan and writes them to the target server, while tracking stats on the write.
func processBatches(statsKey []byte, w avalanche.LineProtocolWriter, batchChan <-chan *bytes.Buffer) {
	// Fields to hold write stats.
	latField := river.Int{Name: []byte("latNs")}
	successField := river.Bool{Name: []byte("ok")}
	payloadField := river.Int{Name: []byte("payloadBytes")}
	fields := []river.Field{&latField, &successField, &payloadField}

	for batch := range batchChan {
		// Write the batch.
		latNs, err := w.WriteLineProtocol(batch.Bytes())
		if err != nil {
			logger.Printf("Error writing: %s\n", err.Error())
		}

		// Track stats for the batch.
		statMu.Lock()
		ts := time.Now().UnixNano()
		latField.Value = latNs
		successField.Value = err == nil
		payloadField.Value = int64(batch.Len())
		river.WriteLine(statBuf, statsKey, fields, ts)
		statMu.Unlock()

		// Return the batch buffer to the pool.
		batch.Reset()
		bufPool.Put(batch)
	}
	workersWg.Done()
}

// recordStats periodically tries to flush stats to stats server.
// Also flushes stats if application is shutting down.
func recordStats(statsW avalanche.LineProtocolWriter) {
	t := time.NewTicker(3 * time.Second)
	for {
		select {
		case <-t.C:
			flushStats(statsW)
		case <-quit:
			workersWg.Wait()
			flushStats(statsW)
			t.Stop()
			statsWg.Done()
			return
		}
	}
}

// Send stats to stats server, if there are any stats to write.
func flushStats(statsW avalanche.LineProtocolWriter) {
	// Temporary buffer so we can save stats while workers record stats.
	var buf *bytes.Buffer

	statMu.Lock()
	// If no stats to record, we're done here.
	if statBuf.Len() == 0 {
		statMu.Unlock()
		return
	}

	// There are stats to record, so hold on to the previous buffer and make a new one.
	buf = statBuf
	statBuf = bufPool.Get().(*bytes.Buffer)
	statMu.Unlock()

	if _, err := statsW.WriteLineProtocol(buf.Bytes()); err != nil {
		logger.Printf("Error writing stats: %s", err.Error())
	}

	// Return the stats buffer to the pool.
	buf.Reset()
	bufPool.Put(buf)
}

// waitForInterrupt initiates shutdown on ctrl-c or input EOF.
func waitForInterrupt() {
	ctrlC := make(chan os.Signal, 1)
	signal.Notify(ctrlC, os.Interrupt)
	select {
	case <-ctrlC:
		logger.Println("Interrupted, beginning graceful shutdown...")
		shutdown()
	case <-inputDone:
		logger.Println("End of input, beginning graceful shutdown...")
		shutdown()
	}
}

// shutdown signals to other goroutines to shut down.
// If the other goroutines don't finish in time, forcefully shut down the application.
func shutdown() {
	done := make(chan struct{})

	go func() {
		close(batchChan)
		close(quit)
		workersWg.Wait()
		statsWg.Wait()
		done <- struct{}{}
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		logger.Printf("Graceful shutdown not completed in time. Aborting...")
		os.Exit(1)
	}

	os.Exit(0)
}
