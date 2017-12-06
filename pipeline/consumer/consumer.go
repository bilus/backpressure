package consumer

import (
	"context"
	"github.com/bilus/backpressure/colors"
	"github.com/bilus/backpressure/metrics"
	"github.com/bilus/backpressure/pipeline/batch"
	"github.com/bilus/backpressure/pipeline/bucket"
	"github.com/bilus/backpressure/pipeline/permit"
	"log"
	"sync"
)

// TODO: Retries.
// ASK: This is Archai so maybe we should keep trying forever until it gets it shit together.
// ASK: How to make that shit idempotent:
// - generate ids for each data point and on suspected failure, ask Archai if it's there 😼
// - have Archai by adding an operation id to each operation and a way to query Archai for its status

func Run(ctx context.Context, batchConsumer batch.Consumer, batchCh <-chan batch.Batch, permitCh chan<- permit.Permit, metrics metrics.Metrics, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		buckt := bucket.New(permitCh, 0, 1) // Always ask for one batch if it drops to 0.
		if err := buckt.FillUp(ctx); err != nil {
			log.Printf(colors.Yellow("Exiting consumer: %v"), err)
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
					if err := batchConsumer.ConsumeBatch(ctx, batch); err != nil {
						log.Printf(colors.Red("Write error: %v (will retry)"), err)
						metrics.EndWithFailure(batchSize)
					} else {
						metrics.EndWithSuccess(batchSize)

					}
					if err := buckt.Drain(ctx, 1); err != nil {
						log.Printf(colors.Yellow("Exiting consumer: %v"), err)
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
