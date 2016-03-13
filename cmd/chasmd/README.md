# chasmd

`chasmd` is a "black hole" InfluxDB imitation using the `chasm` package.
Currently, it only supports HTTP writes.

`chasmd` is useful to get a sense of how much load an InfluxDB client can generate when there is minimal request processing overhead.

When you start `chasmd`, it will log out the number of HTTP requests, lines, and bytes accepted.
