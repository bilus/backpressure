# Backpressure

## Setup

```bash
dep ensure
go run main.go
```


## TODO

- Push example
- Too many tasks produced
- Hangs on exit
- Pull example
- Wait to interrupt http server
- Wait to interrupt writing to chan in push handler
- When Done() all messages should go through the pipeline.
- Automated test
- Make PipelineMetrics a map => metrics.PipelineMetrics.
- Rename metrics.Metrics to metrics.Counters.

- RPS to basic metrics
- DD metrics example
- Turn into a library.
- Pub/sub reader.
- Archai writer.

- User-defined backpressure strategy for dispatch
- Measure idle consumer time
- Retries
- Some simple recovery of lost permits(s) to prevent deadlocks.
