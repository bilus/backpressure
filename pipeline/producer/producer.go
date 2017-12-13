package producer

import (
	"context"
	"github.com/bilus/backpressure/colors"
	"github.com/bilus/backpressure/metrics"
	"github.com/bilus/backpressure/pipeline/permit"
	"github.com/bilus/backpressure/pipeline/task"
	"google.golang.org/api/iterator"
	"log"
	"sync"
	"time"
)

type Config struct {
	TaskBufferSize      int
	ShutdownGracePeriod time.Duration
}

func DefaultConfig() *Config {
	return &Config{
		TaskBufferSize:      10,
		ShutdownGracePeriod: time.Second * 30,
	}
}

func (c *Config) WithTaskBuffer(size int) *Config {
	c.TaskBufferSize = size
	return c
}

func (c *Config) WithGracePeriod(d time.Duration) *Config {
	c.ShutdownGracePeriod = d
	return c
}

// ASK:
// How much producer cares about whether data was actually written or not?
//   - Very much. 200 or 201 -> data was written (have to wait for write to finish).
//     Use 'completion ports' using chans = request & response.
//   - Not much. Just put it into the pipeline and respond with 202 Accepted (we'll do our best).

func Go(ctx context.Context, config Config, taskProducer task.Producer, metrics metrics.Metrics, wg *sync.WaitGroup) (<-chan task.Task, chan<- permit.Permit) {
	return run(ctx, taskProducer, config.TaskBufferSize, config.ShutdownGracePeriod, metrics, wg)
}

func run(ctx context.Context, taskProducer task.Producer, taskChanSize int, shutdownGracePeriod time.Duration, metrics metrics.Metrics, wg *sync.WaitGroup) (chan task.Task, chan permit.Permit) {
	taskCh := make(chan task.Task, taskChanSize)
	permitCh := make(chan permit.Permit, 1) // Needs to be closed by the caller.

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(taskCh)
		for {
			log.Println(colors.Blue("Obtaining permit..."))
			select {
			case permit := <-permitCh:
				log.Printf(colors.Blue("Got permit: %v"), permit)
				err := produceWithinQuota(ctx, permit, taskProducer, taskCh, shutdownGracePeriod, metrics)
				if err == iterator.Done {
					log.Printf(colors.Blue("Exiting producer: %v"), ctx.Err())
					return
				}
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
		err = produceTask(ctx, taskProducer, taskCh, shutdownGracePeriod, metrics)
		if err == nil {
			remaining--
		}
		if ctx.Err() != nil {
			log.Printf(colors.Blue("Exiting producer: %v"), ctx.Err())
			return err
		} else if err == iterator.Done {
			return err
		} else if err != nil {
			log.Printf(colors.Red("Error producing task: %v"), err) // TODO: Will retry producing indefinitely.
		}
	}
	return nil
}

func produceTask(ctx context.Context, taskProducer task.Producer, taskCh chan<- task.Task, gracePeriod time.Duration, metrics metrics.Metrics) (err error) {
	span := metrics.Begin(0) // Start measuring time but no task yet.
	defer span.Close(&err)
	var newTask task.Task
	newTask, err = taskProducer.ProduceTask(ctx)
	if err == nil {
		span.Continue(1) // We have a task.
		err = queueTask(ctx, taskCh, newTask, gracePeriod)
	}
	return err
}

func queueTask(ctx context.Context, taskCh chan<- task.Task, task task.Task, gracePeriod time.Duration) error {
	// TODO: Unsure if this elaborate timeout is necessary; the pipeline will be terminated after the grace period anyway.
	select {
	case taskCh <- task:
		return nil
	case <-ctx.Done():
		log.Println(colors.Blue("Last orders, please!"))
		select {
		// Last desperate attempt.
		case taskCh <- task:
			return nil
		case <-time.After(gracePeriod):
			return ctx.Err()
		}
	}
}
