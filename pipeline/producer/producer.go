package producer

import (
	"context"
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
				produceWithinQuota(ctx, permit, taskProducer, taskCh, shutdownGracePeriod, metrics)
			case <-ctx.Done():
				log.Println(colors.Blue("Exiting producer"))
				return
			}
		}
	}()

	return taskCh, permitCh
}

func produceWithinQuota(ctx context.Context, permit permit.Permit, taskProducer task.Producer, taskCh chan<- task.Task, shutdownGracePeriod time.Duration, metrics metrics.Metrics) (err error) {
	remaining := permit.SizeHint
	for remaining > 0 {
		span := metrics.Begin(0) // Start measuring time but no task yet.
		defer span.Close(&err)
		var newTask task.Task
		newTask, err = taskProducer.ProduceTask(ctx)
		if err == nil {
			span.Continue(1) // We have a task.
			err = queueTask(ctx, taskCh, newTask, shutdownGracePeriod)
		}
		if err == nil {
			remaining--
		}
		if ctx.Err() != nil {
			log.Printf(colors.Blue("Exiting producer: %v"), ctx.Err())
			return err
		} else if err != nil {
			log.Printf(colors.Red("Error producing task: %v"), err) // TODO: Will retry producing indefinitely.
		}
	}
	return nil
}

func queueTask(ctx context.Context, taskCh chan<- task.Task, task task.Task, gracePeriod time.Duration) error {
	log.Printf(colors.Blue("=> Sending %v"), task)
	// TODO: Unsure if this elaborate timeout is necessary; the pipeline will be terminated after the grace period anyway.
	select {
	case taskCh <- task:
		return nil
	case <-ctx.Done():
		log.Printf(colors.Blue("=> Sending %v (last orders, please)"), task)
		select {
		// Last desperate attempt.
		case taskCh <- task:
			return nil
		case <-time.After(gracePeriod):
			return ctx.Err()
		}
	}
}
