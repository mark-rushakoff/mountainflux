// Package trail implements an efficient, concurrency-safe way to track one
// InfluxDB line protocol point at a time.
package trail

import (
	"bytes"
	"sync"

	"github.com/mark-rushakoff/mountainflux/avalanche"
	"github.com/mark-rushakoff/mountainflux/river"
)

// Trail holds InfluxDB line protocol points in a buffer until being flushed.
type Trail struct {
	payloads chan *bytes.Buffer

	wg      sync.WaitGroup
	errChan chan error

	mu  sync.Mutex
	buf *bytes.Buffer
}

// New returns a new instance of Trail.
func New(ws []avalanche.LineProtocolWriter) *Trail {
	t := &Trail{
		payloads: make(chan *bytes.Buffer, len(ws)),
		errChan:  make(chan error, len(ws)),
		buf:      bufPool.Get().(*bytes.Buffer),
	}

	t.wg.Add(len(ws))
	for _, w := range ws {
		go t.process(w)
	}

	return t
}

// Flush flushes the Trail's internal buffer, which will then be written by whichever
// avalanche.Writer picks it up first.
func (t *Trail) Flush() {
	t.mu.Lock()
	t.payloads <- t.buf
	t.buf = bufPool.Get().(*bytes.Buffer)
	t.mu.Unlock()
}

// Close flushes the Trail and waits for all its writers to finish.
// Trail must not be used after being closed.
func (t *Trail) Close() {
	t.Flush()
	close(t.payloads)
	t.wg.Wait()
}

// WriteLine writes the line protocol line described by seriesKey, fields, and time
// to t's internal buffer.
func (t *Trail) WriteLine(seriesKey []byte, fields []river.Field, time int64) {
	t.mu.Lock()
	// Safe to discard this error because river.WriteLine would only return an error
	// from writing to the io.Writer; and bytes.Buffer does not fail on writes.
	_ = river.WriteLine(t.buf, seriesKey, fields, time)
	t.mu.Unlock()
}

func (t *Trail) process(lpw avalanche.LineProtocolWriter) {
	for buf := range t.payloads {
		if err := lpw.WriteLineProtocol(buf.Bytes()); err != nil {
			t.errChan <- err
		}
		bufPool.Put(buf)
	}

	t.wg.Done()
}

var bufPool = sync.Pool{
	New: func() interface{} {
		return bytes.NewBuffer(make([]byte, 0, 2048))
	},
}
