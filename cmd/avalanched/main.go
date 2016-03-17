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

	workersWg sync.WaitGroup
	statsWg   sync.WaitGroup
	bufPool   = sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer(make([]byte, 0, 4*1024))
		},
	}

	// Input scanner sends buffers over this channel, writer workers read from it
	blocksChan chan *bytes.Buffer

	quit      = make(chan struct{})
	inputDone = make(chan struct{})

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

	blocksChan = make(chan *bytes.Buffer, *numWorkers)

	statsWg.Add(1)
	go recordStats(avalanche.NewHTTPWriter(avalanche.HTTPWriterConfig{
		Host:     "http://" + *statsURL,
		Database: *statsDatabase,
	}))

	c := avalanche.HTTPWriterConfig{
		Host:     "http://" + *url,
		Database: *database,
	}
	workersWg.Add(*numWorkers)
	for i := 0; i < *numWorkers; i++ {
		w := avalanche.NewHTTPWriter(c)
		go processBlocks([]byte(*statsKey), w, blocksChan)
	}
	logger.Println("Beginning writes to", c.Host)

	go scan(*linesPerBatch)

	waitForInterrupt()
}

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
			blocksChan <- buf
			buf = bufPool.Get().(*bytes.Buffer)
		}
	}

	if err := scanner.Err(); err != nil {
		logger.Fatalf("Error reading input: %s", err.Error())
	}

	close(inputDone)
}

func processBlocks(statsKey []byte, w avalanche.LineProtocolWriter, blocksChan <-chan *bytes.Buffer) {
	latField := river.Int{Name: []byte("latNs")}
	successField := river.Bool{Name: []byte("ok")}
	payloadField := river.Int{Name: []byte("payloadBytes")}
	fields := []river.Field{&latField, &successField, &payloadField}

	for block := range blocksChan {
		latNs, err := w.WriteLineProtocol(block.Bytes())
		if err != nil {
			logger.Printf("Error writing: %s\n", err.Error())
		}

		statMu.Lock()
		ts := time.Now().UnixNano()
		latField.Value = latNs
		successField.Value = err == nil
		payloadField.Value = int64(block.Len())
		river.WriteLine(statBuf, statsKey, fields, ts)
		statMu.Unlock()
	}
	workersWg.Done()
}

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

func flushStats(statsW avalanche.LineProtocolWriter) {
	statMu.Lock()
	if statBuf.Len() > 0 {
		if _, err := statsW.WriteLineProtocol(statBuf.Bytes()); err != nil {
			logger.Printf("Error writing stats: %s", err.Error())
		}

		statBuf.Reset()
	}
	statMu.Unlock()
}

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

func shutdown() {
	done := make(chan struct{})

	go func() {
		close(blocksChan)
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
