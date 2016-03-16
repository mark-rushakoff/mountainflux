# mountainflux

[![GoDoc](https://godoc.org/github.com/mark-rushakoff/mountainflux?status.svg)](https://godoc.org/github.com/mark-rushakoff/mountainflux)

Tools to generate workloads against InfluxDB.

## Packages

### avalanche

InfluxDB client capable of generating workloads for InfluxDB.

### chasm

API-compatible InfluxDB server to be used for benchmarking avalanche or other InfluxDB clients.

### river

Efficiently generate points in line protocol format.

## Commands

### avalanched

Experimental load generator using `avalanche`.

### chasmd

Command-line-accessible chasm server that will log out statistics around writes consumed.
