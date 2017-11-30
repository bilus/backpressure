package pipeline

import (
	"github.com/nowthisnews/dp-pubsub-archai/metrics"
	"golang.org/x/net/context"
	"log"
	"sync"
)

type BatchConsumer interface {
	ConsumeBatch(batch Batch) error
}

func Consume(ctx context.Context, batchConsumer BatchConsumer, batchCh chan Batch, permitCh chan Permit, metrics *metrics.Metrics, wg *sync.WaitGroup) {
	go func() {
		wg.Add(1)
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				log.Println(yellow("Exiting consumer"))
				return
			default:
				select {
				case batch, ok := <-batchCh:
					batchSize := uint64(len(batch))
					metrics.Begin(batchSize)
					if !ok {
						log.Println(yellow("Exiting consumer"))
						metrics.EndWithFailure(batchSize) // TODO: Flush what's in batch!
						return
					}

					// TODO: Retries.
					// ASK: This is Archai so maybe we should keep trying forever until it gets it shit together.
					// ASK: How to make that shit idempotent:
					// - generate ids for each data point and on suspected failure, ask Archai if it's there 😼
					// - have Archai by adding an operation id to each operation and a way to query Archai for its status
					err := batchConsumer.ConsumeBatch(batch)
					// Assumes writes are idempotent.
					if err != nil {
						log.Printf(red("Write error: %v (will retry)"), err)
						metrics.EndWithFailure(batchSize)
					} else {
						metrics.EndWithSuccess(batchSize)

					}
					select {
					case permitCh <- NewPermit(1):
					case <-ctx.Done():
						log.Println(yellow("Exiting consumer"))
						return
					}
				case <-ctx.Done():
					log.Println(yellow("Exiting consumer"))
					return

				}
			}
		}
	}()
}
