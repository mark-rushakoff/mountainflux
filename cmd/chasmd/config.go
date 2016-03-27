package main

import (
	"bytes"
	"os"
	"text/template"
)

const sampleConfigText = `[http]
# Bind address for HTTP server.
bind = "0.0.0.0:8086"

# Stats can be collected about each HTTP connection received.
# Comment out or remove the stats section if you don't want to track stats.
[stats]
# InfluxDB host for where to send the stats
host = "http://192.0.2.1:8086"

# Target database for stats
database = "chasmd"

# Template for series key when sending stats.
# Valid functions in template: pid
# TODO: Add env function
series-key = "chasmd,pid={{pid}}"

# How may stats to collect before sending
batch-size = 100

# How many workers to report stats
workers = 4
`

type chasmConfig struct {
	HTTP  httpConfig  `toml:"http,omitempty"`
	Stats statsConfig `toml:"stats,omitempty"`
}

type httpConfig struct {
	Bind string `toml:"bind"`

	// TODO: response latency configuration?
}

type statsConfig struct {
	Host       string `toml:"host"`
	Database   string `toml:"database"`
	SeriesKey  string `toml:"series-key"`
	BatchSize  int    `toml:"batch-size"`
	NumWorkers int    `toml:"workers"`
}

func (s *statsConfig) FinalizeSeriesKey() {
	t := template.Must(
		template.New("series key").Funcs(template.FuncMap{
			"pid": os.Getpid,
		}).Parse(s.SeriesKey),
	)

	var buf bytes.Buffer
	if err := t.Execute(&buf, nil); err != nil {
		panic(err)
	}
	s.SeriesKey = buf.String()
}
