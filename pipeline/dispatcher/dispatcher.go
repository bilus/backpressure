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

type Config struct {
	Tick time.Duration
	HighWaterMark int
	LowWaterMark int
	BatchingPolicy batch.BatchingPolicy
}

func DefaultConfig() Config {
	maxSize := 10
	return Config{
		Tick: time.Millisecond * 100,
		HighWaterMark: maxSize,
		LowWaterMark: maxSize / 2,
		BatchingPolicy: batch.NewDropping(maxSize),
	}
}

func (c Config) WithTick(tick time.Duration) Config {
	c.Tick = tick
	return c
}

func (c Config) WithSlidingPolicy(maxSize int) Config {
	c.BatchingPolicy = batch.NewSliding(maxSize)
	c.HighWaterMark = maxSize
	c.LowWaterMark = maxSize / 2
	return c
}

func (c Config) WithDroppingPolicy(maxSize int) Config {
	c.BatchingPolicy = batch.NewDropping(maxSize)
	c.HighWaterMark = maxSize
	c.LowWaterMark = maxSize / 2
	return c
}

func Go(ctx context.Context, config Config, taskCh <-chan task.Task, taskPermitCh chan<- permit.Permit, metrics metrics.Metrics, wg *sync.WaitGroup) (chan batch.Batch, chan permit.Permit) {
	return run(ctx, config.Tick, config.HighWaterMark, config.LowWaterMark, taskCh, taskPermitCh, config.BatchingPolicy, metrics, wg)
}

// As far as metrics are concerned, it tracks avg time between completion of dispatches divided in the number of tasks in a batch.
func run(ctx context.Context, tick time.Duration, highWaterMark int, lowWaterMark int, taskCh <-chan task.Task, taskPermitCh chan<- permit.Permit, buffer batch.BatchingPolicy, metrics metrics.Metrics, wg *sync.WaitGroup) (chan batch.Batch, chan permit.Permit) {
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
		ticker := time.NewTicker(tick)
		defer ticker.Stop()
		currentSpan := metrics.Begin(0)
		defer drainAndClose(buffer, taskCh, batchCh, currentSpan, metrics)
		for {
			select {
			case task, ok := <-taskCh:
				if ok {
					bufferTask(task, buffer, currentSpan)
					if err := buckt.Drain(ctx, 1); err != nil {
						log.Printf(colors.Magenta("Exiting dispatcher: %v"), err)
						return
					}
				}
			case <-ticker.C:
				if !buffer.IsEmpty() {
					select {
					case <-permitCh:
						currentSpan = flushBuffer(buffer, batchCh, currentSpan, metrics)
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

func bufferTask(task task.Task, buffer batch.BatchingPolicy, currentSpan metrics.Span) error {
	currentSpan.Continue(1)
	if err := buffer.AddTask(task); err != nil {
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

func flushBuffer(buffer batch.BatchingPolicy, batchCh chan<- batch.Batch, currentSpan metrics.Span, metrics metrics.Metrics) metrics.Span {
	currentBatch := buffer.GetBatch()
	if len(currentBatch) > 0 {
		log.Println("Flushing to batch chan", len(currentBatch))
		batchCh <- currentBatch
	}
	currentSpan.Success(uint64(len(currentBatch)))
	newSpan := metrics.Begin(0)
	buffer.Reset()
	return newSpan
}

func drainAndClose(buffer batch.BatchingPolicy, taskCh <-chan task.Task, batchCh chan<- batch.Batch, currentSpan metrics.Span, metrics metrics.Metrics) {
	log.Println("Draining task chan")
	for task := range taskCh {
		bufferTask(task, buffer, currentSpan)
	}
	// Ignore permits, just try to push it through.
	flushBuffer(buffer, batchCh, currentSpan, metrics)
	close(batchCh)
}
