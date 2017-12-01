# Google Pub/Sub -> Archai

## Setup

```bash
dep ensure
go run main.go
```


## TODO

- Push example
- Pull example
- Automated test
- Refactor.


- RPS to basic metrics
- DD metrics example
- Turn into a library.
- Pub/sub reader.
- Archai writer.

- User-defined backpressure strategy for dispatch
- Measure idle consumer time
- Retries
- Some simple recovery of lost permits(s) to prevent deadlocks.
