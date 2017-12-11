package consumer

import (
	"context"
	"errors"
	"github.com/bilus/backpressure/colors"
	"github.com/bilus/backpressure/metrics"
	"github.com/bilus/backpressure/pipeline/batch"
	"github.com/bilus/backpressure/pipeline/bucket"
	"github.com/bilus/backpressure/pipeline/permit"
	"log"
	"sync"
)

type Config struct{}

func DefaultConfig() *Config {
	return &Config{}
}

func Go(ctx context.Context, _ Config, batchConsumer batch.Consumer, batchCh <-chan batch.Batch, permitCh chan<- permit.Permit, metrics metrics.Metrics, wg *sync.WaitGroup) {
	run(ctx, batchConsumer, batchCh, permitCh, metrics, wg)
}

// TODO: Retries.
// ASK: This is Archai so maybe we should keep trying forever until it gets it shit together.
// ASK: How to make that shit idempotent:
// - generate ids for each data point and on suspected failure, ask Archai if it's there 😼
// - have Archai by adding an operation id to each operation and a way to query Archai for its status

// As far as metrics are concerned, it tracks the average time it takes to consume a task.
func run(ctx context.Context, batchConsumer batch.Consumer, batchCh <-chan batch.Batch, permitCh chan<- permit.Permit, metrics metrics.Metrics, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		buckt := bucket.New(permitCh, 0, 1) // Always ask for one batch if it drops to 0.
		if err := buckt.FillUp(ctx); err != nil {
			log.Printf(colors.Yellow("Exiting consumer: %v"), err)
			return
		}
		defer drain(ctx, batchConsumer, batchCh, metrics)
		for {
			if err := pullBatch(ctx, batchConsumer, batchCh, metrics); err != nil {
				if ctx.Err() != nil {
					log.Println(colors.Yellow("Exiting consumer"))
					return
				}
			}
			if err := buckt.Drain(ctx, 1); err != nil {
				log.Printf(colors.Yellow("Exiting consumer: %v"), err)
				return
			}
		}
	}()
}

func pullBatch(ctx context.Context, batchConsumer batch.Consumer, batchCh <-chan batch.Batch, metrics metrics.Metrics) error {
	select {
	case batch, ok := <-batchCh:
		if !ok {
			return errors.New("Upstream channel closed")
		}
		return consumeBatch(ctx, batch, batchConsumer, metrics)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func consumeBatch(ctx context.Context, batch batch.Batch, batchConsumer batch.Consumer, metrics metrics.Metrics) (err error) {
	batchSize := uint64(len(batch))
	span := metrics.Begin(batchSize)
	defer span.Close(&err)
	if err := batchConsumer.ConsumeBatch(ctx, batch); err != nil {
		log.Printf(colors.Red("Consume error: %v (will retry)"), err)
		return err
	} else {
		return nil
	}
}

func drain(ctx context.Context, batchConsumer batch.Consumer, batchCh <-chan batch.Batch, metrics metrics.Metrics) {
	log.Println("Draining batch chan")
	// Try to drain the batch channel before exiting.
	batch, ok := <-batchCh
	if ok {
		err := consumeBatch(ctx, batch, batchConsumer, metrics)
		log.Printf("Drained batch chan: %v", err)
	} else {
		log.Println("Drained batch chan: empty")
	}
}
