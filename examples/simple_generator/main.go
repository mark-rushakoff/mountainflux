package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/mark-rushakoff/mountainflux/river"
)

func main() {
	numLines := flag.Int("lines", 0, "Number of lines to generate")
	defaultSeriesKey := string(river.SeriesKey("tmp", map[string]string{
		"pid": fmt.Sprintf("%d", os.Getpid()),
	}))
	seriesKey := flag.String("seriesKey", defaultSeriesKey, "Series key to use in output")
	flag.Parse()

	sk := []byte(*seriesKey)
	ctrField := river.Int{Name: []byte("ctr")}
	fields := []river.Field{&ctrField}
	max := *numLines
	for i := 0; max == 0 || i < max; i++ {
		ctrField.Value = int64(i)
		river.WriteLine(os.Stdout, sk, fields, time.Now().UnixNano())
	}
}
