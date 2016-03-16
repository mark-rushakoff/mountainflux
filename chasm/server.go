// Package chasm provides an API-compatible InfluxDB server where you can throw all your writes into a bottomless pit.
package chasm

import (
	"bytes"
	"net"
	"sync"
	"sync/atomic"

	"github.com/valyala/fasthttp"
)

// Config describes all the configuration for a Server.
type Config struct {
	HTTPConfig *HTTPConfig
}

// HTTPConfig describes the configuration for an HTTP server.
type HTTPConfig struct {
	// TCP address to listen to, e.g. `:8086` or `0.0.0.0:8086`
	Bind string
}

// Server is a fake InfluxDB server.
type Server struct {
	// HTTPURL is the read-only full address of this server after binding to the configured address,
	// e.g. "http://example.com:8086"
	HTTPURL string

	httpListener         net.Listener
	httpRequestsAccepted uint64
	httpLinesAccepted    uint64
	httpBytesAccepted    uint64

	wg   sync.WaitGroup
	quit chan struct{}
}

// NewServer returns a new Server based on the supplied Config.
func NewServer(c Config) (*Server, error) {
	s := &Server{
		quit: make(chan struct{}),
	}

	if c.HTTPConfig != nil {
		var err error
		s.httpListener, err = net.Listen("tcp", c.HTTPConfig.Bind)
		if err != nil {
			return nil, err
		}
		s.HTTPURL = "http://" + s.httpListener.Addr().String()
	}

	return s, nil
}

// Serve starts all the configured sub-servers in their own goroutines.
func (s *Server) Serve() {
	if s.httpListener != nil {
		s.wg.Add(1)
		go s.serveHTTP()
	}
}

// Close attempts to gracefully shutdown all the started sub-servers.
func (s *Server) Close() {
	close(s.quit)
	s.wg.Wait()
}

// Stats contains information about the load the server has accepted.
type Stats struct {
	RequestsAccepted uint64
	BytesAccepted    uint64
	LinesAccepted    uint64
}

// HTTPStats returns stats for the HTTP server contained in this Server.
func (s *Server) HTTPStats() Stats {
	return Stats{
		RequestsAccepted: atomic.LoadUint64(&s.httpRequestsAccepted),
		BytesAccepted:    atomic.LoadUint64(&s.httpBytesAccepted),
		LinesAccepted:    atomic.LoadUint64(&s.httpLinesAccepted),
	}
}

func (s *Server) serveHTTP() {
	// fasthttp.Server is intended to be opened forever.
	// The only obvious way to close one is to close its listener.
	// Since we want our Server to close gracefully, we'll handle the listener.

	fastServer := &fasthttp.Server{
		Handler: s.fasthttpHandler,
	}

	go func() {
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
	atomic.AddUint64(&s.httpRequestsAccepted, 1)
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

	body := ctx.PostBody()
	atomic.AddUint64(&s.httpBytesAccepted, uint64(len(body)))
	atomic.AddUint64(&s.httpLinesAccepted, uint64(bytes.Count(body, lineDelimiter)))
	ctx.Response.SetStatusCode(fasthttp.StatusNoContent)
}
