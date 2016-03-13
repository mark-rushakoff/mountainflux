package avalanche

import (
	"fmt"

	"github.com/valyala/fasthttp"
)

type HTTPWriterConfig struct {
	Host string

	Generator Generator
}

type HTTPWriter struct {
	client fasthttp.Client

	c   HTTPWriterConfig
	url []byte
}

func NewHTTPWriter(c HTTPWriterConfig) Writer {
	return &HTTPWriter{
		client: fasthttp.Client{
			Name: "avalanche",
		},

		c:   c,
		url: []byte(c.Host + "/write"),
	}
}

var post = []byte("POST")

func (w *HTTPWriter) Write() error {
	req := fasthttp.AcquireRequest()
	req.Header.SetMethodBytes(post)
	req.Header.SetRequestURIBytes(w.url)
	req.SetBodyStream(w.c.Generator(), -1)

	resp := fasthttp.AcquireResponse()
	err := w.client.Do(req, resp)
	if err == nil {
		sc := resp.StatusCode()
		if sc != fasthttp.StatusNoContent {
			err = fmt.Errorf("Invalid write response (status %d): %s", sc, resp.Body())
		}
	}

	fasthttp.ReleaseResponse(resp)
	fasthttp.ReleaseRequest(req)

	return err
}
