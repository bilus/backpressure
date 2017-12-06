# Backpressure

## Setup

```bash
dep ensure
go run main.go
```


## TODO

- Add Avg times to basic metrics
  - Discrepancies in dispatcher counts
- When Done() all messages should go through the pipeline.
  + Batch chan
  - Task chan
- Wait to interrupt writing to chan in push handler
- Automated test
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
