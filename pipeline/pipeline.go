package pipeline

import (
	"github.com/fatih/color"
	"github.com/nowthisnews/dp-pubsub-archai/metrics"
	"golang.org/x/net/context"
	"log"
	"math/rand"
	"sync"
	"time"
)

var (
	blue    = color.New(color.FgBlue).SprintFunc()
	yellow  = color.New(color.FgYellow).SprintFunc()
	red     = color.New(color.FgRed).SprintFunc()
	magenta = color.New(color.FgMagenta).SprintFunc()
	green   = color.New(color.FgGreen).SprintFunc()
	cyan    = color.New(color.FgCyan).SprintFunc()
)

func Run(ctx context.Context, tick time.Duration, wg *sync.WaitGroup) {
	wg.Add(3)
	producerMetrics := metrics.New()
	taskCh, taskPermitCh := produce(ctx, &producerMetrics, wg)
	dispatcherMetrics := metrics.New()
	batchCh, batchPermitCh := dispatch(ctx, tick, taskCh, taskPermitCh, &dispatcherMetrics, wg)
	consumerMetrics := metrics.New()
	consume(ctx, batchCh, batchPermitCh, &consumerMetrics, wg)
	ReportPeriodically(ctx, time.Second*5, &producerMetrics, &dispatcherMetrics, &consumerMetrics, wg)
}

type Task int64
type Batch []Task

// ASK: There are many ways we could improve that. Ideas:
//   - Extend it to provide hints about max batch size.
//   - Use token bucket algorithm to control the rate.
//   - Pipe permit channel directly to producer to throttle it; that would give better control
//     over how much data to pull from pub/sub and when.
//   - Interesting: http://bytopia.org/2016/09/14/implementing-leaky-channels/
type Permit struct {
	SizeHint int
}

func NewBatch() Batch {
	return make(Batch, 0)
}

func NewPermit(sizeHint int) Permit {
	return Permit{sizeHint}
}

func (batch Batch) AddTask(task Task) (Batch, error) {
	// TODO: Either use fixed-size batch or slices but do
	// limit the maximum batch size here.
	newBatch := append(batch, task)
	return newBatch, nil
}

// ASK:
// How much producer cares about whether data was actually written or not?
//   - Very much. 200 or 201 -> data was written (have to wait for write to finish).
//     Use 'completion ports' using chans = request & response.
//   - Not much. Just put it into the pipeline and respond with 202 Accepted (we'll do our best).

// TODO:
// - Measure idle consumer time
// - Wait to stop pipeline
// - Cleanly handle TERM
// - Retries
// - Batching permits
// - Some simple recovery of lost permits(s) to prevent deadlocks.

func produce(ctx context.Context, metrics *metrics.Metrics, wg *sync.WaitGroup) (chan Task, chan Permit) {
	out := make(chan Task)
	permitCh := make(chan Permit, 1)

	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return

			default:
				log.Printf(blue("Obtaining permit..."))
				permit := <-permitCh
				log.Printf(blue("Got permit: %v"), permit)
				remaining := permit.SizeHint
				for remaining > 0 {
					time.Sleep(time.Second)
					task := Task(rand.Int63())
					metrics.Begin(1)
					log.Printf(blue("=> Sending %v"), task)
					select {
					case <-ctx.Done():
						return
					case out <- task:
						metrics.EndWithSuccess(1)
						remaining -= 1
						log.Printf(blue("=> OK, permits remaining: {%v}"), remaining)
					case <-time.After(time.Second * 5):
						metrics.EndWithFailure(1)
						log.Println(red("=> Timeout in client"))
					}
				}
			}
		}
	}()

	return out, permitCh
}

func dispatch(ctx context.Context, tick time.Duration, taskCh chan Task, taskPermitChan chan Permit, metrics *metrics.Metrics, wg *sync.WaitGroup) (chan Batch, chan Permit) {
	batchCh := make(chan Batch)
	permitCh := make(chan Permit, 1)

	batchSize := 4
	lowWaterMark := 2

	go func() {
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

func consume(ctx context.Context, batchCh chan Batch, permitCh chan Permit, metrics *metrics.Metrics, wg *sync.WaitGroup) {
	go func() {
		defer wg.Done()
		for {
			select {

			case <-ctx.Done():
				return
			default:
				batch, ok := <-batchCh
				batchSize := uint64(len(batch))
				metrics.Begin(batchSize)
				if !ok {
					log.Printf(green("Consumer is exiting"))
					metrics.EndWithFailure(batchSize) // TODO: Flush what's in batch!
					return
				}

				// TODO: Retries.
				err := write(batch)
				permitCh <- NewPermit(1)
				// Assumes writes are idempotent.
				if err != nil {
					log.Printf(red("Write error: %v (will retry)"), err)
					metrics.EndWithFailure(batchSize)
				} else {
					metrics.EndWithSuccess(batchSize)

				}
			}
		}
	}()
}

func write(batch Batch) error {
	log.Println(yellow("<= Writing..."))
	time.Sleep(time.Second * 10)
	log.Printf(yellow("<= %v"), batch)
	return nil
}
