package consumer

import (
	"github.com/bilus/backpressure/colors"
	"github.com/bilus/backpressure/metrics"
	"github.com/bilus/backpressure/pipeline/batch"
	"github.com/bilus/backpressure/pipeline/permit"
	"golang.org/x/net/context"
	"log"
	"sync"
)

func Run(ctx context.Context, batchConsumer batch.Consumer, batchCh <-chan batch.Batch, permitCh chan<- permit.Permit, metrics metrics.Metrics, wg *sync.WaitGroup) {
	go func() {
		wg.Add(1)
		defer wg.Done()
		initialPermit := permit.New(1)
		select {
		case permitCh <- initialPermit:
		case <-ctx.Done():
			log.Println(colors.Yellow("Exiting consumer"))
			return
		}

		for {
			select {
			case <-ctx.Done():
				log.Println(colors.Yellow("Exiting consumer"))
				return
			default:
				select {
				case batch, ok := <-batchCh:
					batchSize := uint64(len(batch))
					metrics.Begin(batchSize)
					if !ok {
						log.Println(colors.Yellow("Exiting consumer"))
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
						log.Printf(colors.Red("Write error: %v (will retry)"), err)
						metrics.EndWithFailure(batchSize)
					} else {
						metrics.EndWithSuccess(batchSize)

					}
					select {
					case permitCh <- permit.New(1):
					case <-ctx.Done():
						log.Println(colors.Yellow("Exiting consumer"))
						return
					}
				case <-ctx.Done():
					log.Println(colors.Yellow("Exiting consumer"))
					return

				}
			}
		}
	}()
}
