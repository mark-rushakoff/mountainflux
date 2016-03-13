package avalanche

import (
	"fmt"
	"io/ioutil"
	"net/http"
)

type HTTPWriterConfig struct {
	Host string

	Generator Generator
}

type HTTPWriter struct {
	c HTTPWriterConfig
}

func NewHTTPWriter(c HTTPWriterConfig) Writer {
	return &HTTPWriter{c: c}
}

func (w *HTTPWriter) Write() error {
	g := w.c.Generator()

	resp, err := http.Post(w.c.Host+"/write", "", g)
	if err != nil {
		return err
	}

	// NoContent is the only acceptable status.
	// OK responses can have errors, and non-200 is another class of error.
	if resp.StatusCode != http.StatusNoContent {
		// Already received invalid status code,
		// don't care if something goes wrong reading the response body
		b, _ := ioutil.ReadAll(resp.Body)
		return fmt.Errorf("Invalid write response (status %d): %s", resp.StatusCode, b)
	}

	return nil
}
