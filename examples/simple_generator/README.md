# simple_generator

simple_generator will print out lines of format:

```
tmp,pid=29530 ctr=0i 1458507416458236266
tmp,pid=29530 ctr=1i 1458507416458271007
tmp,pid=29530 ctr=2i 1458507416458305828
tmp,pid=29530 ctr=3i 1458507416458315412
tmp,pid=29530 ctr=4i 1458507416458318639
tmp,pid=29530 ctr=5i 1458507416458321697
tmp,pid=29530 ctr=6i 1458507416458324789
tmp,pid=29530 ctr=7i 1458507416458327878
tmp,pid=29530 ctr=8i 1458507416458330988
tmp,pid=29530 ctr=9i 1458507416458335184
...
```

The number of lines is configurable with the `-lines` flag.

The series key (`tmp,pid=29530`) will automatically use the current PID.
You can override the series key with the `-seriesKey` flag.

## Typical usage

Run the generator, piping it into `avalanched` with options that make sense for you:

```sh
go run examples/simple_generator/main.go -lines 100000 |
  avalanched \
    -httpurl target.example.com:8086 \
    -database tmp \
    -linesPerBatch 1000 \
    -statsurl stats.example.com:8086 \
    -statsdb perf
```

Then query the insert latencies with:

```sh
influx \
  -host stats.example.com \
  -execute "SELECT latNs FROM perf..avalanched WHERE pid = '30530'" \
  -format csv |
    tail -n +2 | cut -d, -f3
```
