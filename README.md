# Backpressure

## Setup

```bash
dep ensure
go run main.go
```


## TODO

- Fix dropping tasks under pressure
- Structured logging https://github.com/sirupsen/logrus
- Make PipelineMetrics a map => metrics.PipelineMetrics.
- Rename metrics.Metrics to metrics.Counters.

- DD metrics example

- User-defined backpressure strategy for dispatch
- Use defer for permits.
- Retries
- Some simple recovery of lost permits(s) to prevent deadlocks.
