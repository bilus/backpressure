package pipeline

import (
	"github.com/nowthisnews/dp-pubsub-archai/metrics"
	"golang.org/x/net/context"
	"log"
	"sync"
	"time"
)

func Dispatch(ctx context.Context, tick time.Duration, highWaterMark int, lowWaterMark int, taskCh chan Task, taskPermitChan chan Permit,
	metrics *metrics.Metrics, wg *sync.WaitGroup) (chan Batch, chan Permit) {
	if highWaterMark == lowWaterMark {
		panic("Dispatch highWaterMark must be higher than lowWaterMark")
	}
	batchCh := make(chan Batch)
	permitCh := make(chan Permit, 1)
	go func() {
		wg.Add(1)
		defer wg.Done()
		initialPermit := NewPermit(highWaterMark)
		log.Printf(magenta("Sending permit: %v"), initialPermit)
		select {
		case taskPermitChan <- initialPermit:
		case <-ctx.Done():
			log.Println(magenta("Exiting dispatcher"))
			return
		}
		waterLevel := initialPermit.SizeHint
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
				}
				waterLevel -= 1
				if waterLevel <= lowWaterMark {
					newPermit := NewPermit(highWaterMark - waterLevel)
					log.Printf(magenta("Sending permit: %v"), newPermit)
					select {
					case taskPermitChan <- newPermit:
						waterLevel = highWaterMark
					case <-ctx.Done():
						log.Println(magenta("Exiting dispatcher"))
						return
					}
				}
			case <-ticker:
				if len(batch) > 0 {
					batchCh <- batch
					select {
					case <-permitCh:
						batch = NewBatch()
					case <-ctx.Done():
						log.Println(magenta("Exiting dispatcher"))
						return

					}
				}
			case <-ctx.Done():
				log.Println(magenta("Exiting dispatcher"))
				return
			}

		}
	}()

	return batchCh, permitCh
}
