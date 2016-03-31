# chasmd

`chasmd` is a "black hole" InfluxDB imitation using the `chasm` package.

It's an HTTP server with a `/write` endpoint, that ​_acts like_​ an InfluxDB server, but actually just discards the data.
Currently, it only supports HTTP writes.

`chasmd` is useful to get a sense of the theoretical maximum throughput 
an InfluxDB client can generate when there is minimal request processing overhead.

When you start `chasmd`, it will log out the number of HTTP requests, lines, and bytes accepted.
