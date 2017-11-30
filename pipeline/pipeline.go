package pipeline

import (
	// "golang.org/x/net/context"
	"github.com/fatih/color"
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

// Use WorkGroup.
func Run(tick time.Duration) {
	wg := sync.WaitGroup{}
	wg.Add(2)
	taskCh := produce(&wg)
	batchCh, tokenCh := dispatch(tick, taskCh, &wg)
	consume(batchCh, tokenCh, &wg)

	// Just to stop when tracing.
	barrierCh := make(chan struct{})
	go func() {
		wg.Wait()
		barrierCh <- struct{}{}
	}()
	select {
	case <-barrierCh:
		log.Println(green("Pipeline completed"))
	case <-time.After(time.Second * 30):
		log.Println(red("Timeout waiting for finish"))
	}
}

type Task int64
type Batch []Task

// ASK: There are many ways we could improve that. Ideas:
//   - Extend it to provide hints about max batch size.
//   - Use real token bucket algorithm to control the rate.
//   - Pipe token channel directly to producer to throttle it (but then we may need to
//     use something close to adaptive token buffer); that would give better control
//     over how much data to pull from pub/sub and when.
//   - Interesting: http://bytopia.org/2016/09/14/implementing-leaky-channels/
type Token struct{}

func NewBatch() Batch {
	return make(Batch, 0)
}

func NewToken() Token {
	return Token{}
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
// - Cleanly handle TERM
// - Retries
// - Batching tokens/permits
// - Some simple recovery of lost token(s) to prevent deadlocks.

func produce(wg *sync.WaitGroup) chan Task {
	out := make(chan Task)

	go func() {
		defer wg.Done()
		for {
			log.Println(blue("=> Sleeping..."))
			time.Sleep(time.Second)
			task := Task(rand.Int63())
			log.Printf(blue("=> Sending %v"), task)
			select {
			case out <- task:
				log.Println(blue("=> OK"))
			case <-time.After(time.Second * 5):
				log.Println(red("=> Timeout in client"))
			}

		}

	}()

	return out
}

func dispatch(tick time.Duration, taskCh chan Task, wg *sync.WaitGroup) (chan Batch, chan Token) {
	batchCh := make(chan Batch)
	tokenCh := make(chan Token, 1)

	go func() {
		defer wg.Done()
		ticker := time.Tick(tick)
		batch := NewBatch()
		for {
			select {
			case <-ticker:
				batchCh <- batch
				<-tokenCh
				batch = NewBatch()
			case task := <-taskCh:
				// Handle termination, flush current batch.
				var err error
				batch, err = batch.AddTask(task)
				if err != nil {
					// Unable to buffer more tasks, drop the current one to the floor.
					// ASK: Maybe round-robin is a better choice?
					// Probably should use a Strategy here to make that
					// configurable.
					log.Printf(red("Dropping task: %v"), err)
				}
			}

		}
	}()

	return batchCh, tokenCh
}

// TODO: Measure idle time.
func consume(batchCh chan Batch, tokenCh chan Token, wg *sync.WaitGroup) {
	go func() {
		defer wg.Done()
		for {
			batch, ok := <-batchCh
			if !ok {
				log.Printf(green("Consumer is exiting"))
				return
			}

			// TODO: Retries.
			err := write(batch)
			tokenCh <- NewToken()
			// Assumes writes are idempotent.
			if err != nil {
				log.Printf(red("Write error: %v (will retry)"), err)
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
