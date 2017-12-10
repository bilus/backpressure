# Backpressure

## Setup

```bash
dep ensure
go run main.go
```


## TODO

- Automated test
  + producer
  + consumer
  + bucket
  + dispatcher
  - pipeline
- Structured logging https://github.com/sirupsen/logrus
- Make PipelineMetrics a map => metrics.PipelineMetrics.
- Rename metrics.Metrics to metrics.Counters.
- Run -> Go
- Limit batch size -- use interface for batch buffering.

- DD metrics example

- User-defined backpressure strategy for dispatch
- Use defer for permits.
- Retries
- Some simple recovery of lost permits(s) to prevent deadlocks.
