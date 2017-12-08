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
  - bucket
  - dispatcher
  - pipeline
- Structured logging https://github.com/sirupsen/logrus
- Use defer for permits.
- Make PipelineMetrics a map => metrics.PipelineMetrics.
- Rename metrics.Metrics to metrics.Counters.

- DD metrics example
- Turn into a library.
- Pub/sub reader.
- Archai writer.

- User-defined backpressure strategy for dispatch
- Measure idle consumer time
- Retries
- Some simple recovery of lost permits(s) to prevent deadlocks.
