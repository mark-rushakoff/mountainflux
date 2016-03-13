package avalanche_test

import (
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mark-rushakoff/mountainflux/avalanche"
)

func TestHTTPWriter_Write(t *testing.T) {
	const line = `cpu,host=h1 usage=99`
	var lastReq string

	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/write" && r.Method == "POST" {
			b, _ := ioutil.ReadAll(r.Body)
			lastReq = string(b)
			w.WriteHeader(http.StatusNoContent)
		}
	})
	s := httptest.NewServer(h)
	defer s.Close()

	c := avalanche.HTTPWriterConfig{
		Host: s.URL,
		Generator: func() io.Reader {
			return strings.NewReader(line)
		},
	}
	w := avalanche.NewHTTPWriter(c)

	if err := w.Write(); err != nil {
		t.Fatalf("expected no error, got: %s", err.Error())
	}

	if lastReq != line {
		t.Fatalf("got: %v, exp: %v", lastReq, line)
	}
}
