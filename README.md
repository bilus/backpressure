# Google Pub/Sub -> Archai

## Setup

```bash
dep ensure
go run main.go
```


## TODO

- Refactor. Close channels, may do without Done() the. Senders must close their channels.
- RPS to basic metrics
- Push example
- Pull example
- Automated test

- DD metrics example

- Turn into a library.
- Pub/sub reader.
- Archai writer.

- User-defined backpressure strategy for dispatch
- Measure idle consumer time
- Retries
- Some simple recovery of lost permits(s) to prevent deadlocks.
