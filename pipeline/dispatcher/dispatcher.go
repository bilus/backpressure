package dispatcher

import (
	"context"
	"github.com/bilus/backpressure/colors"
	"github.com/bilus/backpressure/metrics"
	"github.com/bilus/backpressure/pipeline/batch"
	"github.com/bilus/backpressure/pipeline/bucket"
	"github.com/bilus/backpressure/pipeline/permit"
	"github.com/bilus/backpressure/pipeline/task"
	"log"
	"sync"
	"time"
)

// As far as metrics are concerned, it tracks avg time between completion of dispatches divided in the number of tasks in a batch.
func Run(ctx context.Context, tick time.Duration, highWaterMark int, lowWaterMark int, taskCh <-chan task.Task, taskPermitCh chan<- permit.Permit,
	metrics metrics.Metrics, wg *sync.WaitGroup) (chan batch.Batch, chan permit.Permit) {
	batchCh := make(chan batch.Batch)
	permitCh := make(chan permit.Permit, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		buckt := bucket.New(taskPermitCh, lowWaterMark, highWaterMark)
		if err := buckt.FillUp(ctx); err != nil {
			log.Printf(colors.Magenta("Exiting dispatcher: %v"), err)
			return
		}
		ticker := time.Tick(tick)
		currentBatch := batch.New()
		currentSpan := metrics.Begin(0)
		defer drainAndClose(&currentBatch, taskCh, batchCh, currentSpan, metrics)
		for {
			select {
			case task, ok := <-taskCh:
				if ok {
					currentBatch = bufferTask(task, currentBatch, currentSpan)
					if err := buckt.Drain(ctx, 1); err != nil {
						log.Printf(colors.Magenta("Exiting dispatcher: %v"), err)
						return
					}
				}
			case <-ticker:
				if len(currentBatch) > 0 {
					select {
					case <-permitCh:
						currentBatch, currentSpan = flushBuffer(currentBatch, batchCh, currentSpan, metrics)
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

func bufferTask(task task.Task, currentBatch batch.Batch, currentSpan metrics.Span) batch.Batch {
	currentSpan.Continue(1)
	var err error
	currentBatch, err = currentBatch.AddTask(task)
	if err != nil {
		currentSpan.Failure(1)
		// Unable to buffer more tasks, drop the current one to the floor.
		// ASK: Maybe round-robin is a better choice?
		// Probably should use a Strategy here to make that
		// configurable.
		log.Printf(colors.Red("Dropping task: %v"), err)
	}
	return currentBatch
}

func flushBuffer(currentBatch batch.Batch, batchCh chan<- batch.Batch, currentSpan metrics.Span, metrics metrics.Metrics) (batch.Batch, metrics.Span) {
	log.Println("Flushing to batch chan", len(currentBatch))
	batchCh <- currentBatch
	currentSpan.Success(uint64(len(currentBatch)))
	newSpan := metrics.Begin(0)
	newBatch := batch.New()
	return newBatch, newSpan
}

func drainAndClose(currentBatch *batch.Batch, taskCh <-chan task.Task, batchCh chan<- batch.Batch, currentSpan metrics.Span, metrics metrics.Metrics) {
	log.Println("Draining task chan")
	completeBatch := *currentBatch
	for {
		task, ok := <-taskCh
		if ok {
			completeBatch = bufferTask(task, completeBatch, currentSpan)
		} else {
			break
		}
	}
	// Ignore permits, just try to push it through.
	flushBuffer(completeBatch, batchCh, currentSpan, metrics)
	close(batchCh)
}
