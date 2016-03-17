package mountainflux_test

import (
	"bytes"
	"testing"

	"github.com/mark-rushakoff/mountainflux/avalanche"
	"github.com/mark-rushakoff/mountainflux/chasm"
)

func benchmarkHTTPSmallPoints(numLines int, b *testing.B) {
	s, err := chasm.NewServer(chasm.Config{
		HTTPConfig: &chasm.HTTPConfig{
			Bind: "localhost:0",
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	s.Serve()
	defer s.Close()

	lines := bytes.Repeat([]byte("cpu,host=h1 usage=99\n"), numLines)
	w := avalanche.NewHTTPWriter(avalanche.HTTPWriterConfig{
		Host:     s.HTTPURL,
		Database: "d",
	})

	var expBytes, expLines uint64
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := w.WriteLineProtocol(lines); err != nil {
			b.Fatalf("expected no error, got %s", err.Error())
		}
		expBytes += uint64(len(lines))
		expLines += uint64(numLines)

		stats := s.HTTPStats()

		if n := stats.BytesAccepted; n != expBytes {
			b.Fatalf("bytes accepted: exp %d, got %d", expBytes, n)
		}

		if l := stats.LinesAccepted; l != expLines {
			b.Fatalf("lines accepted: exp %d, got %d", expLines, l)
		}
	}
	b.SetBytes(int64(expBytes))
}

func BenchmarkHTTPSmallPoints1(b *testing.B)     { benchmarkHTTPSmallPoints(1, b) }
func BenchmarkHTTPSmallPoints2(b *testing.B)     { benchmarkHTTPSmallPoints(2, b) }
func BenchmarkHTTPSmallPoints4(b *testing.B)     { benchmarkHTTPSmallPoints(4, b) }
func BenchmarkHTTPSmallPoints8(b *testing.B)     { benchmarkHTTPSmallPoints(8, b) }
func BenchmarkHTTPSmallPoints16(b *testing.B)    { benchmarkHTTPSmallPoints(16, b) }
func BenchmarkHTTPSmallPoints32(b *testing.B)    { benchmarkHTTPSmallPoints(32, b) }
func BenchmarkHTTPSmallPoints64(b *testing.B)    { benchmarkHTTPSmallPoints(64, b) }
func BenchmarkHTTPSmallPoints128(b *testing.B)   { benchmarkHTTPSmallPoints(128, b) }
func BenchmarkHTTPSmallPoints256(b *testing.B)   { benchmarkHTTPSmallPoints(256, b) }
func BenchmarkHTTPSmallPoints512(b *testing.B)   { benchmarkHTTPSmallPoints(512, b) }
func BenchmarkHTTPSmallPoints1024(b *testing.B)  { benchmarkHTTPSmallPoints(1024, b) }
func BenchmarkHTTPSmallPoints2048(b *testing.B)  { benchmarkHTTPSmallPoints(2048, b) }
func BenchmarkHTTPSmallPoints4096(b *testing.B)  { benchmarkHTTPSmallPoints(4096, b) }
func BenchmarkHTTPSmallPoints8192(b *testing.B)  { benchmarkHTTPSmallPoints(8192, b) }
func BenchmarkHTTPSmallPoints16384(b *testing.B) { benchmarkHTTPSmallPoints(16384, b) }
func BenchmarkHTTPSmallPoints32768(b *testing.B) { benchmarkHTTPSmallPoints(32768, b) }
func BenchmarkHTTPSmallPoints65536(b *testing.B) { benchmarkHTTPSmallPoints(65536, b) }
