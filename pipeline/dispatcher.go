package pipeline

import (
	"github.com/nowthisnews/dp-pubsub-archai/metrics"
	"golang.org/x/net/context"
	"log"
	"sync"
	"time"
)

func Dispatch(ctx context.Context, tick time.Duration, taskCh chan Task, taskPermitChan chan Permit, metrics *metrics.Metrics, wg *sync.WaitGroup) (chan Batch, chan Permit) {
	batchCh := make(chan Batch)
	permitCh := make(chan Permit, 1)

	batchSize := 4
	lowWaterMark := 2

	go func() {
		wg.Add(1)
		defer wg.Done()
		initialPermit := NewPermit(batchSize)
		log.Printf(magenta("Sending permit: %v"), initialPermit)
		taskPermitChan <- initialPermit
		ticker := time.Tick(tick)
		batch := NewBatch()
		for {
			select {
			case task := <-taskCh:
				metrics.Begin(1)
				// Handle termination, flush current batch.
				var err error
				batch, err = batch.AddTask(task)
				if err != nil {
					metrics.EndWithFailure(1)
					// Unable to buffer more tasks, drop the current one to the floor.
					// ASK: Maybe round-robin is a better choice?
					// Probably should use a Strategy here to make that
					// configurable.
					log.Printf(red("Dropping task: %v"), err)
				} else {
					metrics.EndWithSuccess(1)
					if len(batch) == lowWaterMark {
						newPermit := NewPermit(batchSize - len(batch))
						log.Printf(magenta("Sending permit: %v"), newPermit)
						taskPermitChan <- newPermit
					}
				}
			case <-ticker:
				batchCh <- batch
				<-permitCh
				batch = NewBatch()
			case <-ctx.Done():
				return
			}

		}
	}()

	return batchCh, permitCh
}
