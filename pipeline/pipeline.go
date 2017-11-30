package pipeline

import (
	// "golang.org/x/net/context"
	"fmt"
	"github.com/fatih/color"
	"github.com/olekukonko/tablewriter"
	"log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
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

func Run(tick time.Duration, wg *sync.WaitGroup) {
	wg.Add(3)
	producerMetrics := NewMetrics()
	taskCh, taskPermitCh := produce(&producerMetrics, wg)
	dispatcherMetrics := NewMetrics()
	batchCh, batchPermitCh := dispatch(tick, taskCh, taskPermitCh, &dispatcherMetrics, wg)
	consumerMetrics := NewMetrics()
	consume(batchCh, batchPermitCh, &consumerMetrics, wg)

	go func() {
		defer wg.Done()
		for {
			time.Sleep(time.Second * 5)
			ReportMetrics(producerMetrics, dispatcherMetrics, consumerMetrics)
		}
	}()
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

type Metrics struct {
	Iterations uint64
	Successes  uint64
	Failures   uint64
}

func NewMetrics() Metrics {
	return Metrics{0, 0, 0}
}

func (metric *Metrics) Begin(delta uint64) {
	atomic.AddUint64(&metric.Iterations, delta)
}

func (metric *Metrics) EndWithSuccess(delta uint64) {
	atomic.AddUint64(&metric.Successes, delta)
}

func (metric *Metrics) EndWithFailure(delta uint64) {
	atomic.AddUint64(&metric.Failures, delta)
}

func ReportMetrics(producerMetrics, dispatcherMetrics, consumerMetrics Metrics) {
	report([]string{"source", "iterations", "successes", "failures"},
		[][]string{
			[]string{"producer", fmt.Sprintf("%v", producerMetrics.Iterations), fmt.Sprintf("%v", producerMetrics.Successes), fmt.Sprintf("%v", producerMetrics.Failures)},
			[]string{"dispatch", fmt.Sprintf("%v", dispatcherMetrics.Iterations), fmt.Sprintf("%v", dispatcherMetrics.Successes), fmt.Sprintf("%v", dispatcherMetrics.Failures)},
			[]string{"consumer", fmt.Sprintf("%v", consumerMetrics.Iterations), fmt.Sprintf("%v", consumerMetrics.Successes), fmt.Sprintf("%v", consumerMetrics.Failures)},
		},
	)
}

func report(header []string, data [][]string) {
	table := tablewriter.NewWriter(os.Stderr) // TODO: Write to log.
	table.SetHeader(header)
	for _, row := range data {
		table.Append(row)
	}
	table.Render()
}

// ASK:
// How much producer cares about whether data was actually written or not?
//   - Very much. 200 or 201 -> data was written (have to wait for write to finish).
//     Use 'completion ports' using chans = request & response.
//   - Not much. Just put it into the pipeline and respond with 202 Accepted (we'll do our best).

// TODO:
// - Measure idle consumer time
// - Cleanly handle TERM
// - Retries
// - Batching permits
// - Some simple recovery of lost permits(s) to prevent deadlocks.

func produce(metrics *Metrics, wg *sync.WaitGroup) (chan Task, chan Permit) {
	out := make(chan Task)
	permitCh := make(chan Permit, 1)

	go func() {
		defer wg.Done()
		for {
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
	}()

	return out, permitCh
}

func dispatch(tick time.Duration, taskCh chan Task, taskPermitChan chan Permit, metrics *Metrics, wg *sync.WaitGroup) (chan Batch, chan Permit) {
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
			case <-ticker:
				batchCh <- batch
				<-permitCh
				batch = NewBatch()
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
			}

		}
	}()

	return batchCh, permitCh
}

// TODO: Measure idle time.
func consume(batchCh chan Batch, permitCh chan Permit, metrics *Metrics, wg *sync.WaitGroup) {
	go func() {
		defer wg.Done()
		for {
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
	}()
}

func write(batch Batch) error {
	log.Println(yellow("<= Writing..."))
	time.Sleep(time.Second * 10)
	log.Printf(yellow("<= %v"), batch)
	return nil
}
