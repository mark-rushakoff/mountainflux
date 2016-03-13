package avalanche_test

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mark-rushakoff/mountainflux/avalanche"
)

func TestHTTPWriter_Write(t *testing.T) {
	line := []byte(`cpu,host=h1 usage=99`)

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
		Generator: func() []byte {
			return line
		},
	}
	w := avalanche.NewHTTPWriter(c)

	if err := w.Write(); err != nil {
		t.Fatalf("expected no error, got: %s", err.Error())
	}

	if lastReq != string(line) {
		t.Fatalf("got: %v, exp: %v", lastReq, line)
	}
}
