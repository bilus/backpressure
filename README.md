# Google Pub/Sub -> Archai

## Setup

```bash
dep ensure
go run main.go
```


## TODO

- Fix task chan size
- User-defined backpressure strategy for dispatch
- Multiple producers
- Abstract task (?)
- Measure idle consumer time
- Wait to stop pipeline
- Retries
- Batching permits
- Some simple recovery of lost permits(s) to prevent deadlocks.
