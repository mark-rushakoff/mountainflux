package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/mark-rushakoff/mountainflux/chasm"
)

var logger = log.New(os.Stdout, "[chasmd] ", log.LstdFlags)

func main() {
	bind := flag.String("httpbind", "0.0.0.0:8086", "TCP bind address for HTTP server")
	flag.Parse()

	c := chasm.Config{
		HTTPConfig: &chasm.HTTPConfig{
			Bind: *bind,
		},
	}

	s, err := chasm.NewServer(c)
	if err != nil {
		logger.Fatal("Unexpected error: " + err.Error())
	}

	s.Serve()
	logger.Println("HTTP server listening on " + s.HTTPURL)

	ctrlC := make(chan os.Signal, 1)
	signal.Notify(ctrlC, os.Interrupt)

	statTicker := time.NewTicker(time.Second)
	for {
		select {
		case <-ctrlC:
			shutdown(s)
		case <-statTicker.C:
			logger.Printf(
				"http_requests_accepted=%d http_bytes_accepted=%d http_lines_accepted=%d\n",
				s.HTTPRequestsAccepted(), s.HTTPBytesAccepted(), s.HTTPLinesAccepted(),
			)
		}
	}
}

func shutdown(s *chasm.Server) {
	logger.Printf("Interrupted, beginning graceful shutdown...\n")

	done := make(chan struct{})

	go func() {
		s.Close()
		done <- struct{}{}
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		logger.Printf("Graceful shutdown not completed in time. Aborting...")
	}

	os.Exit(0)
}
