package producer

import (
	"context"
	"errors"
	"github.com/bilus/backpressure/colors"
	"github.com/bilus/backpressure/metrics"
	"github.com/bilus/backpressure/pipeline/permit"
	"github.com/bilus/backpressure/pipeline/task"
	"log"
	"sync"
	"time"
)

// ASK:
// How much producer cares about whether data was actually written or not?
//   - Very much. 200 or 201 -> data was written (have to wait for write to finish).
//     Use 'completion ports' using chans = request & response.
//   - Not much. Just put it into the pipeline and respond with 202 Accepted (we'll do our best).

func Run(ctx context.Context, taskProducer task.Producer, taskChanSize int, shutdownGracePeriod time.Duration, metrics metrics.Metrics, wg *sync.WaitGroup) (chan task.Task, chan permit.Permit) {
	taskCh := make(chan task.Task, taskChanSize)
	permitCh := make(chan permit.Permit, 1) // Needs to be closed by the caller.

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(taskCh)
		for {
			log.Printf(colors.Blue("Obtaining permit..."))
			select {
			case permit := <-permitCh:
				log.Printf(colors.Blue("Got permit: %v"), permit)
				remaining := permit.SizeHint
				for remaining > 0 {
					// TODO: Refactor.
					newTask, err := taskProducer.ProduceTask(ctx)
					if err != nil {
						if ctx.Err() != nil {
							log.Println(colors.Blue("Exiting producer"))
							return
						} else {
							log.Println(colors.Red("Error producing task: %v"), err)
						}
					} else {
						select {
						case err := <-queueTask(ctx, taskCh, newTask, shutdownGracePeriod, metrics):
							if err != nil {
								log.Println(colors.Red("Error queuing task: %v"), err)
							} else {
								remaining--
							}
						case <-ctx.Done():
							log.Println(colors.Blue("Exiting producer"))
							return
						}
					}
				}
			case <-ctx.Done():
				log.Println(colors.Blue("Exiting producer"))
				return
			}
		}
	}()

	return taskCh, permitCh
}

func queueTask(ctx context.Context, taskCh chan<- task.Task, task task.Task, gracePeriod time.Duration, metrics metrics.Metrics) <-chan error {
	errCh := make(chan error, 1)
	log.Printf(colors.Blue("=> Sending %v"), task)
	metrics.Begin(1)
	select {
	case taskCh <- task:
		metrics.EndWithSuccess(1)
		errCh <- nil
	case <-ctx.Done():
		log.Printf(colors.Blue("=> Sending %v (last orders, please)"), task)
		select {
		// Last desperate attempt.
		case taskCh <- task:
			metrics.EndWithSuccess(1)
			errCh <- nil
		case <-time.After(gracePeriod):
			metrics.EndWithFailure(1)
			errCh <- errors.New("Timeout during shutdown")
		}
	}
	return errCh
}
