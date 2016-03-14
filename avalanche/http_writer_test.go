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
		if r.URL.Path == "/write" && r.Method == "POST" && r.URL.Query().Get("db") == "mydb" {
			b, _ := ioutil.ReadAll(r.Body)
			lastReq = string(b)
			w.WriteHeader(http.StatusNoContent)
		} else {
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte("Unexpected URL: " + r.URL.String()))
		}
	})
	s := httptest.NewServer(h)
	defer s.Close()

	c := avalanche.HTTPWriterConfig{
		Host:     s.URL,
		Database: "mydb",
	}
	w := avalanche.NewHTTPWriter(c)

	if err := w.WriteLineProtocol(line); err != nil {
		t.Fatalf("expected no error, got: %s", err.Error())
	}

	if lastReq != string(line) {
		t.Fatalf("got: %v, exp: %v", lastReq, line)
	}
}
