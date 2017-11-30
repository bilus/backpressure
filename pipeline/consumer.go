package pipeline

import (
	"github.com/nowthisnews/dp-pubsub-archai/metrics"
	"golang.org/x/net/context"
	"log"
	"sync"
	"time"
)

func Consume(ctx context.Context, batchCh chan Batch, permitCh chan Permit, metrics *metrics.Metrics, wg *sync.WaitGroup) {
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				batch, ok := <-batchCh
				batchSize := uint64(len(batch))
				metrics.Begin(batchSize)
				if !ok {
					log.Printf(green("Consumer is exiting"))
					metrics.EndWithFailure(batchSize) // TODO: Flush what's in batch!
					return
				}

				// TODO: Retries.
				// ASK: This is Archai so maybe we should keep trying for n minutes and then just crash the service?
				err := write(batch)
				permitCh <- NewPermit(1)
				// Assumes writes are idempotent.
				if err != nil {
					log.Printf(red("Write error: %v (will retry)"), err)
					metrics.EndWithFailure(batchSize)
				} else {
					metrics.EndWithSuccess(batchSize)

				}
			}
		}
	}()
}

func write(batch Batch) error {
	log.Println(yellow("<= Writing..."))
	time.Sleep(time.Second * 10)
	log.Printf(yellow("<= %v"), batch)
	return nil
}
