package trail_test

import (
	"bytes"
	"testing"

	"github.com/mark-rushakoff/mountainflux/avalanche"
	"github.com/mark-rushakoff/mountainflux/river"
	"github.com/mark-rushakoff/mountainflux/trail"
)

func TestTrail_Flush(t *testing.T) {
	fw := fakeWriter{Chan: make(chan []byte)}
	tr := trail.New([]avalanche.LineProtocolWriter{fw})
	defer tr.Close()

	tr.WriteLine([]byte("cpu,host=h1"), []river.Field{river.Int{Name: []byte("usage"), Value: 99}}, 123)
	if len(fw.Chan) != 0 {
		t.Fatalf("exp no writes before Flush-ing but a write occurred")
	}

	tr.Flush()
	b := <-fw.Chan

	exp := "cpu,host=h1 usage=99i 123\n"
	if !bytes.Equal(b, []byte(exp)) {
		t.Fatalf("exp write: %s, got write: %s", exp, b)
	}
}

type fakeWriter struct {
	Chan chan []byte
}

func (w fakeWriter) WriteLineProtocol(b []byte) error {
	w.Chan <- b
	return nil
}
