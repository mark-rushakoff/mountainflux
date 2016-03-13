# avalanched

`avalanched` is a proof-of-concept load generator for InfluxDB using the `avalanche` package.

When you start the command, it will repeatedly write batched points to the given host until you press ctrl-c.

The point looks like

```
avalanche,pid=<PID> ctr=<COUNTER> <CURRENT_NANOSECONDS>
```

The database is configurable via the command line.

This command is more of a demonstration of how to use avalanche and less of a full-featured load generator.
