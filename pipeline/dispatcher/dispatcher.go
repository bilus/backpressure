package dispatcher

import (
	"github.com/bilus/backpressure/colors"
	"github.com/bilus/backpressure/metrics"
	"github.com/bilus/backpressure/pipeline/batch"
	"github.com/bilus/backpressure/pipeline/permit"
	"github.com/bilus/backpressure/pipeline/task"
	"golang.org/x/net/context"
	"log"
	"sync"
	"time"
)

func Run(ctx context.Context, tick time.Duration, highWaterMark int, lowWaterMark int, taskCh <-chan task.Task, taskPermitCh chan permit.Permit,
	metrics metrics.Metrics, wg *sync.WaitGroup) (chan batch.Batch, chan permit.Permit) {
	if highWaterMark == lowWaterMark {
		panic("Dispatch highWaterMark must be higher than lowWaterMark")
	}
	batchCh := make(chan batch.Batch)
	permitCh := make(chan permit.Permit, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		initialPermit := permit.New(highWaterMark)
		log.Printf(colors.Magenta("Sending permit: %v"), initialPermit)
		select {
		case taskPermitCh <- initialPermit:
		case <-ctx.Done():
			log.Println(colors.Magenta("Exiting dispatcher"))
			return
		}
		waterLevel := initialPermit.SizeHint
		ticker := time.Tick(tick)
		currentBatch := batch.New()
		for {
			select {
			case task := <-taskCh:
				metrics.Begin(1)
				// TODO: Handle termination, flush current batch!
				var err error
				currentBatch, err = currentBatch.AddTask(task)
				if err != nil {
					metrics.EndWithFailure(1)
					// Unable to buffer more tasks, drop the current one to the floor.
					// ASK: Maybe round-robin is a better choice?
					// Probably should use a Strategy here to make that
					// configurable.
					log.Printf(colors.Red("Dropping task: %v"), err)
				} else {
					metrics.EndWithSuccess(1)
				}
				waterLevel -= 1
				if waterLevel <= lowWaterMark {
					newPermit := permit.New(highWaterMark - waterLevel)
					log.Printf(colors.Magenta("Sending permit: %v"), newPermit)
					select {
					case taskPermitCh <- newPermit:
						waterLevel = highWaterMark
					case <-ctx.Done():
						log.Println(colors.Magenta("Exiting dispatcher"))
						return
					}
				}
			case <-ticker:
				if len(currentBatch) > 0 {
					select {
					case <-permitCh:
						batchCh <- currentBatch
						currentBatch = batch.New()
					case <-ctx.Done():
						log.Println(colors.Magenta("Exiting dispatcher"))
						return

					}
				}
			case <-ctx.Done():
				log.Println(colors.Magenta("Exiting dispatcher"))
				return
			}

		}
	}()

	return batchCh, permitCh
}
