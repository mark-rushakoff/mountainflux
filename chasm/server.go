package chasm

import (
	"bytes"
	"net"
	"sync/atomic"

	"github.com/valyala/fasthttp"
)

var (
	writePath     = []byte("/write")
	lineDelimiter = []byte("\n")
)

type Config struct {
	HTTPConfig *HTTPConfig
}

type HTTPConfig struct {
	// TCP address to listen to, e.g. `:8086` or `0.0.0.0:8086`
	Bind string
}

type Server struct {
	HTTPURL string

	httpListener      net.Listener
	httpLinesAccepted uint64
	httpBytesAccepted uint64

	quit chan struct{}
}

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

func (s *Server) Serve() {
	if s.httpListener != nil {
		go s.serveHTTP()
	}
}

func (s *Server) Close() {
	close(s.quit)
}

func (s *Server) HTTPBytesAccepted() uint64 {
	return atomic.LoadUint64(&s.httpBytesAccepted)
}

func (s *Server) HTTPLinesAccepted() uint64 {
	return atomic.LoadUint64(&s.httpLinesAccepted)
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
}

func (s *Server) fasthttpHandler(ctx *fasthttp.RequestCtx) {
	if !ctx.IsPost() || !bytes.Equal(ctx.Path(), writePath) {
		ctx.Response.SetStatusCode(fasthttp.StatusNotFound)
		return
	}

	body := ctx.PostBody()
	atomic.AddUint64(&s.httpBytesAccepted, uint64(len(body)))
	atomic.AddUint64(&s.httpLinesAccepted, uint64(bytes.Count(body, lineDelimiter)))
	ctx.Response.SetStatusCode(fasthttp.StatusNoContent)
}
