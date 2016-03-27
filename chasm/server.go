// Package chasm provides an API-compatible InfluxDB server where you can throw all your writes into a bottomless pit.
package chasm

import (
	"bytes"
	"net"
	"sync"
	"time"

	"github.com/valyala/fasthttp"
)

// Config describes all the configuration for a Server.
type Config struct {
	HTTPConfig *HTTPConfig
}

// HTTPConfig describes the configuration for an HTTP server.
type HTTPConfig struct {
	// TCP address to listen to, e.g. `:8086` or `0.0.0.0:8086`
	Bind string `toml:"bind"`
}

// Server is a fake InfluxDB server.
type Server struct {
	// HTTPURL is the read-only full address of this server after binding to the configured address,
	// e.g. "http://example.com:8086"
	HTTPURL string

	httpListener net.Listener

	stats chan Stats

	wg   sync.WaitGroup
	quit chan struct{}
}

// NewServer returns a new Server based on the supplied Config, and a channel from which stats will be sent.
// The caller of NewServer *must* read from that channel, or the server will eventually block due to the channel being full.
func NewServer(c Config) (*Server, <-chan Stats, error) {
	s := &Server{
		stats: make(chan Stats, 1024), // Size of 1024 arbitrarily picked
		quit:  make(chan struct{}),
	}

	if c.HTTPConfig != nil {
		var err error
		s.httpListener, err = net.Listen("tcp", c.HTTPConfig.Bind)
		if err != nil {
			return nil, nil, err
		}
		s.HTTPURL = "http://" + s.httpListener.Addr().String()
	}

	return s, s.stats, nil
}

// Serve starts all the configured sub-servers in their own goroutines.
func (s *Server) Serve() {
	if s.httpListener != nil {
		s.wg.Add(1)
		go s.serveHTTP()
	}
}

// Close attempts to gracefully shutdown all the started sub-servers.
// It also closes the channel returned from NewServer.
func (s *Server) Close() {
	close(s.quit)
	s.wg.Wait()
	close(s.stats)
}

// Stats contains information about a request the server has accepted.
type Stats struct {
	// How many bytes were in the request.
	BytesAccepted int

	// How many lines were in the request.
	LinesAccepted int

	// The time to read the request and prepare the response.
	// Does not include time writing the response to the wire.
	IngestLatency int64

	// Unix time that stat was recorded, in nanoseconds.
	Time int64
}

func (s *Server) serveHTTP() {
	// fasthttp.Server is intended to be opened forever.
	// The only obvious way to close one is to close its listener.
	// Since we want our Server to close gracefully, we'll handle the listener.

	fastServer := &fasthttp.Server{
		Handler: s.fasthttpHandler,
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		err := fastServer.Serve(s.httpListener)
		if err != nil {
			// TODO: Log the error? Restart the server?
			panic(err)
		}
	}()

	<-s.quit
	s.httpListener.Close()
	s.wg.Done()
}

var (
	lineDelimiter    = []byte("\n")
	writePath        = []byte("/write")
	dbKey            = []byte("db")
	missingDbMessage = []byte("database is required")
)

func (s *Server) fasthttpHandler(ctx *fasthttp.RequestCtx) {
	if !bytes.Equal(ctx.Path(), writePath) {
		ctx.Response.SetStatusCode(fasthttp.StatusNotFound)
		return
	}

	if !ctx.IsPost() {
		ctx.Response.SetStatusCode(fasthttp.StatusMethodNotAllowed)
		return
	}

	if args := ctx.QueryArgs(); args == nil || len(args.PeekBytes(dbKey)) == 0 {
		ctx.Response.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.Response.SetBody(missingDbMessage)
		return
	}

	ctx.Response.SetStatusCode(fasthttp.StatusNoContent)

	body := ctx.PostBody()
	s.stats <- Stats{
		BytesAccepted: len(body),
		LinesAccepted: bytes.Count(body, lineDelimiter),
		IngestLatency: time.Since(ctx.ConnTime()).Nanoseconds(),
		Time:          time.Now().UnixNano(),
	}
}
