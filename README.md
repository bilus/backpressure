# Google Pub/Sub -> Archai

## Setup

```bash
dep ensure
go run main.go
```


## TODO

- User-defined backpressure strategy for dispatch
- Multiple producers
- Measure idle consumer time
- Retries
- Some simple recovery of lost permits(s) to prevent deadlocks.
