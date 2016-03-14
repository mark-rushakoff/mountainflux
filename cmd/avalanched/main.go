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
)

var logger = log.New(os.Stdout, "[avalanched] ", log.LstdFlags)

func main() {
	url := flag.String("httpurl", "localhost:8086", "host:port for target HTTP server")
	database := flag.String("database", "", "target database for writes")
	bufSize := flag.Int("bufsize", 65536, "max size of buffer for writes")
	flag.Parse()

	if database == nil || *database == "" {
		logger.Fatalf("no database provided")
	}

	c := avalanche.HTTPWriterConfig{
		Host:     "http://" + *url,
		Database: *database,
	}

	w := avalanche.NewHTTPWriter(c)

	logger.Println("Beginning writes to", c.Host)
	done := make(chan struct{})
	go write(*bufSize, w, done)

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

func write(bufSize int, w avalanche.LineProtocolWriter, done chan struct{}) {
	c := newCounter(bufSize)
	for {
		select {
		case <-done:
			return
		default:
			if err := w.WriteLineProtocol(c.makeBatch()); err != nil {
				logger.Println("write error:", err.Error())
			}
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
