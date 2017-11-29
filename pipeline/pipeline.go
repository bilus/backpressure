package pipeline

import (
	// "golang.org/x/net/context"
	"log"
	"math/rand"
	"sync"
	"time"
)

// Use WorkGroup.
func Run(tick time.Duration) {
	wg := sync.WaitGroup{}
	wg.Add(2)
	taskCh := produce(&wg)
	batchCh := dispatch(tick, taskCh, &wg)
	consume(batchCh, &wg)
	wg.Wait()
}

type Task int64
type Batch []Task

func NewBatch() Batch {
	return make(Batch, 0)
}

func (batch Batch) AddTask(task Task) (Batch, error) {
	// TODO: Either use fixed-size batch or slices but do
	// limit the maximum batch size here.
	newBatch := append(batch, task)
	return newBatch, nil
}

// DISCUSSION
// How much producer cares about whether data was actually written or not?
//   - Very much. 200 or 201 -> data was written (have to wait for write to finish).
//     Use 'completion ports' using chans = request & response.
//   - Not much. Just put it into the pipeline and respond with 202 Accepted (we'll do our best).

// TODO:
// - Backpressure
// - Cleanly handle TERM

func produce(wg *sync.WaitGroup) chan Task {
	out := make(chan Task)

	go func() {
		defer wg.Done()
		for {
			time.Sleep(time.Second * 1)
			out <- Task(rand.Int63())

		}

	}()

	return out
}

func dispatch(tick time.Duration, taskCh chan Task, wg *sync.WaitGroup) chan Batch {
	batchCh := make(chan Batch)

	go func() {
		defer wg.Done()
		ticker := time.Tick(tick)
		batch := NewBatch()
		for {
			select {
			case <-ticker:
				// TODO: Consider adding a timeout here to be able to use strategy
				// vs timeout in producer.
				batchCh <- batch
				batch = NewBatch()
			case task := <-taskCh:
				var err error
				batch, err = batch.AddTask(task)
				if err != nil {
					// Unable to buffer more tasks, drop the current one to the floor.
					// TODO: Discuss whether round-robin isn't a better choice
					// Probably should use a Strategy here to make that
					// configurable.
					log.Printf("Dropping task: %v", err)
				}
			}

		}
	}()

	return batchCh
}

func consume(batchCh chan Batch, wg *sync.WaitGroup) {
	go func() {
		defer wg.Done()
		for {
			batch, ok := <-batchCh
			if !ok {
				log.Printf("Consumer is exiting")
				return
			}

			// TODO: Retries.
			err := write(batch)
			// backCh <- NewBatch()
			// Assumes writes are idempotent.
			if err != nil {
				log.Printf("Write error: %v (will retry)", err)
			}
		}
	}()
}

func write(batch Batch) error {
	log.Println(batch)
	return nil
}
