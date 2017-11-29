package main

import (
	// "golang.org/x/net/context"
	"log"
	"math/rand"
	"time"
)

type Task int64
type Batch []Task

func NewBatch() Batch {
	return make(Batch, 0)
}

func (batch *Batch) AddTask(task Task) (Batch, error) {
	// TODO: Either use fixed-size batch or slices but do
	// limit the maximum batch size here.
	newBatch := append(*batch, task)
	return newBatch, nil
}

func produce() chan Task {
	out := make(chan Task)

	go func() {
		for {
			time.Sleep(time.Second * 1)
			out <- Task(rand.Int63())

		}

	}()

	return out
}

func write(batch *Batch) error {
	log.Println(*batch)
	return nil
}

func consume(taskCh chan Task) {
	ticker := time.Tick(time.Second * 5)
	batch := NewBatch()
	for {
		select {
		case <-ticker:
			err := write(&batch)
			// Assumes writes are idempotent.
			if err != nil {
				log.Printf("Write error: %v (will retry)", err)
			} else {
				batch = NewBatch()
			}
		case task := <-taskCh:
			var err error
			batch, err = batch.AddTask(task)
			if err != nil {
				// Unable to buffer more tasks, drop it to the floor.
				// TODO: Discuss whether round-robin isn't a better choice
				// Probably should use a Strategy here to make that
				// configurable.
				log.Printf("Dropping task: %v", err)

			}
		}

	}
}

func main() {
	taskCh := produce()
	consume(taskCh)
}
