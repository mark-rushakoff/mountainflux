package chasm_test

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/mark-rushakoff/mountainflux/chasm"
)

var httpTests = []struct {
	method      string
	queryParams string
	expStatus   int
}{
	{"POST", "db=x", http.StatusNoContent},
	{"POST", "", http.StatusBadRequest},
	{"GET", "db=x", http.StatusMethodNotAllowed},
}

func TestServer_HTTPWrite(t *testing.T) {
	s, err := chasm.NewServer(chasm.Config{
		HTTPConfig: &chasm.HTTPConfig{
			Bind: "localhost:0",
		},
	})
	if err != nil {
		t.Fatalf("exp no error, got: %s", err.Error())
	}
	s.Serve()
	defer s.Close()

	c := &http.Client{}
	for _, ht := range httpTests {
		var body io.Reader
		if ht.method == "POST" {
			body = strings.NewReader("m f=1")
		}
		req, err := http.NewRequest(ht.method, s.HTTPURL+"/write?"+ht.queryParams, body)
		if err != nil {
			t.Errorf("exp no error, got: %s", err.Error())
			continue
		}

		resp, err := c.Do(req)
		if err != nil {
			t.Errorf("exp no error, got: %s", err.Error())
			continue
		}

		if resp.StatusCode != ht.expStatus {
			t.Errorf("exp status: %d, got: %d", ht.expStatus, resp.StatusCode)
		}
	}
}
