package batch

import (
	"github.com/bilus/backpressure/pipeline/task"
)

type Batch []task.Task

func New() Batch {
	return make(Batch, 0)
}

func (batch Batch) AddTask(task task.Task) (Batch, error) {
	// TODO: Either use fixed-size batch or slices but do
	// limit the maximum batch size here.
	newBatch := append(batch, task)
	return newBatch, nil
}

type Consumer interface {
	ConsumeBatch(batch Batch) error
}
