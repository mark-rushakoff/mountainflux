// Package avalanche creates a massive amount of writes against your target InfluxDB instance.
package avalanche

import (
	"fmt"
	"net/url"
	"time"

	"github.com/valyala/fasthttp"
)

// HTTPWriterConfig is the configuration used to create an HTTPWriter.
type HTTPWriterConfig struct {
	// URL of the host, in form "http://example.com:8086"
	Host string

	// Name of the target database into which points will be written.
	Database string
}

// HTTPWriter is a Writer that writes to an InfluxDB HTTP server.
type HTTPWriter struct {
	client fasthttp.Client

	c   HTTPWriterConfig
	url []byte
}

// NewHTTPWriter returns a new HTTPWriter from the supplied HTTPWriterConfig.
func NewHTTPWriter(c HTTPWriterConfig) LineProtocolWriter {
	return &HTTPWriter{
		client: fasthttp.Client{
			Name: "avalanche",
		},

		c:   c,
		url: []byte(c.Host + "/write?db=" + url.QueryEscape(c.Database)),
	}
}

var (
	post      = []byte("POST")
	textPlain = []byte("text/plain")
)

// WriteLineProtocol writes the given byte slice to the HTTP server described in the Writer's HTTPWriterConfig.
// It returns the latency in nanoseconds and any error received while sending the data over HTTP,
// or it returns a new error if the HTTP response isn't as expected.
func (w *HTTPWriter) WriteLineProtocol(body []byte) (int64, error) {
	req := fasthttp.AcquireRequest()
	req.Header.SetContentTypeBytes(textPlain)
	req.Header.SetMethodBytes(post)
	req.Header.SetRequestURIBytes(w.url)
	req.SetBody(body)

	resp := fasthttp.AcquireResponse()
	start := time.Now()
	err := w.client.Do(req, resp)
	lat := time.Since(start).Nanoseconds()
	if err == nil {
		sc := resp.StatusCode()
		if sc != fasthttp.StatusNoContent {
			err = fmt.Errorf("Invalid write response (status %d): %s", sc, resp.Body())
		}
	}

	fasthttp.ReleaseResponse(resp)
	fasthttp.ReleaseRequest(req)

	return lat, err
}
