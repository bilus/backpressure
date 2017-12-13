# Backpressure

## Setup

```bash
dep ensure
go run main.go
```


## TODO

- Structured logging https://github.com/sirupsen/logrus
- Make PipelineMetrics a map => metrics.PipelineMetrics.
- Fix inconsistent time metrics for dispatcher (are we measuring batcher or are we measuring tasks)

- DD metrics example

- User-defined backpressure strategy for dispatch
- Use defer for permits.
- Retries
- Some simple recovery of lost permits(s) to prevent deadlocks.
