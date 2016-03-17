# avalanched

`avalanched` reads InfluxDB Line Protocol lines from stdin and batches them and sends them to the target host.

You must also specify a stats host so that `avalanched` can report write sizes, latencies, etc.

run `avalanched -help` for more details on command line arguments.

(Note that if you want maximum throughput, you should probably write your own Go code and import the `avalanche` package to use its `LineProtocolWriter`s directly.)
