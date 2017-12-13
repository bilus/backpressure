package dispatcher

import (
	"context"
	"github.com/bilus/backpressure/colors"
	"github.com/bilus/backpressure/metrics"
	"github.com/bilus/backpressure/pipeline/batch"
	"github.com/bilus/backpressure/pipeline/buffer"
	"github.com/bilus/backpressure/pipeline/permit"
	"github.com/bilus/backpressure/pipeline/task"
	"log"
	"sync"
	"time"
)

type Config struct {
	Tick time.Duration
	LowWaterMark int
	BatchingPolicy batch.BatchingPolicy
}

func DefaultConfig() *Config {
	maxSize := 15
	return &Config{
		Tick: time.Millisecond * 100,
		BatchingPolicy: batch.NewDropping(maxSize),
	}
}

func (c *Config) WithTick(tick time.Duration) *Config {
	c.Tick = tick
	return c
}

func (c *Config) WithSlidingPolicy(maxSize int) *Config {
	c.BatchingPolicy = batch.NewSliding(maxSize)
	c.LowWaterMark = maxSize / 2
	return c
}

func (c *Config) WithDroppingPolicy(maxSize int) *Config {
	c.BatchingPolicy = batch.NewDropping(maxSize)
	c.LowWaterMark = maxSize / 2
	return c
}

func Go(ctx context.Context, config Config, taskCh <-chan task.Task, taskPermitCh chan<- permit.Permit, metrics metrics.Metrics, wg *sync.WaitGroup) (chan batch.Batch, chan permit.Permit) {
	return run(ctx, config.Tick, taskCh, taskPermitCh, config.BatchingPolicy, metrics, wg)
}

// As far as metrics are concerned, it tracks avg time between completion of dispatches divided in the number of tasks in a batch.
func run(ctx context.Context, tick time.Duration, taskCh <-chan task.Task, taskPermitCh chan<- permit.Permit, batchingPolicy batch.BatchingPolicy, metrics metrics.Metrics, wg *sync.WaitGroup) (chan batch.Batch, chan permit.Permit) {
	batchCh := make(chan batch.Batch)
	permitCh := make(chan permit.Permit, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		buffer := buffer.New(taskPermitCh, batchingPolicy)
		if err := buffer.Prefill(ctx); err != nil {
			log.Printf(colors.Magenta("Exiting dispatcher: %v"), err)
			return
		}
		ticker := time.NewTicker(tick)
		defer ticker.Stop()
		currentSpan := metrics.Begin(0)
		defer drainAndClose(ctx, buffer, taskCh, batchCh, currentSpan, metrics)
		for {
			select {
			case task, ok := <-taskCh:
				if ok {
					if err := bufferTask(ctx ,task, buffer, currentSpan); err != nil {
						log.Printf(colors.Magenta("Exiting dispatcher: %v"), err)
						return
					}
					if err := buffer.FillUp(ctx); err != nil {
						log.Printf(colors.Magenta("Exiting dispatcher: %v"), err)
						return
					}
				}
			case <-ticker.C:
				if buffer.Size() > 0 {
					select {
					case <-permitCh:
						currentSpan = flushBuffer(ctx, buffer, batchCh, currentSpan, metrics)
						if err := buffer.FillUp(ctx); err != nil {
							log.Printf(colors.Magenta("Exiting dispatcher: %v"), err)
							return
						}
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

func bufferTask(ctx context.Context, task task.Task, buffer *buffer.Buffer, currentSpan metrics.Span) error {
	currentSpan.Continue(1)
	if err := buffer.BufferTask(ctx, task); err != nil {
		// Unable to buffer more tasks, will drop a task to the floor depending on
		// the batching policy. Dropping ANY task from the buffer counts as a failure
		// because it's either the current one (thus an outright failure) or an older task.
		// If the latter's the case, the task was already counted as a success so we
		// record a failure to counter balance.
		currentSpan.Failure(1)
		log.Printf(colors.Red("Dropping task: %v"), err)
		return err
	}
	return nil
}

func flushBuffer(ctx context.Context, buffer *buffer.Buffer, batchCh chan<- batch.Batch, currentSpan metrics.Span, metrics metrics.Metrics) metrics.Span {
	_, err := buffer.Flush(ctx, batchCh)
	currentSpan.Close(&err)
	newSpan := metrics.Begin(0)
	return newSpan
}

func drainAndClose(ctx context.Context, buffer *buffer.Buffer, taskCh <-chan task.Task, batchCh chan<- batch.Batch, currentSpan metrics.Span, metrics metrics.Metrics) {
	// We cannot send permit at this point!
	log.Println("Draining task chan")
	for task := range taskCh {
		bufferTask(ctx, task, buffer, currentSpan)
	}
	// Ignore permits, just try to push it through.
	flushBuffer(ctx, buffer, batchCh, currentSpan, metrics)
	close(batchCh)
}
