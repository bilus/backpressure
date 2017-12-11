package batch

import (
	"context"
	"github.com/bilus/backpressure/pipeline/task"
)

// Batch is a slice of buffered tasks.
type Batch []task.Task

// New creates an empty batch of tasks.
func New() Batch {
	return make(Batch, 0)
}

type Consumer interface {
	ConsumeBatch(ctx context.Context, batch Batch) error
}

// BatchingPolicy defines a strategy for building batches of tasks.
type BatchingPolicy interface {
	// AddTask buffers a task, returning Dropped if a task was dropped as a result.
	AddTask(task task.Task) error
	// GetBatch returns the buffered batch of tasks.
	GetBatch() Batch
	// MaxSize returns the number of tasks the buffer can hold.
	MaxSize() int
	// Size returns the number of buffered tasks.
	Size() int
	// Clears the policy state.
	Reset()
}

// Dropped error indicates a task was dropped to the floor.
type Dropped struct {
	DroppedTask task.Task
}

func (err Dropped) Error() string {
	return "Buffer full, dropped task"
}

// Sliding batching policy maintains a batch of up to MaxSize tasks.
// When full the oldest task will be dropped.
type Sliding struct {
	maxSize int
	batch   Batch
	next    int
	numUsed int
}

// NewSliding creates a new sliding batching policy.
func NewSliding(maxSize int) *Sliding {
	return &Sliding{
		maxSize: maxSize,
		batch:   make(Batch, maxSize),
	}
}

func (s *Sliding) Reset() {
	// We need to create a new slice because the existing one
	// may have already been put onto o channel or referenced
	// in another way.
	s.batch = make(Batch, s.maxSize)
	s.next = 0
	s.numUsed = 0
}

// AddTask adds a new task to the sliding buffer. If the buffer is full it drops
// the oldest task and returns the Dropped error.
func (s *Sliding) AddTask(task task.Task) (err error) {
	if s.numUsed == s.maxSize {
		err = Dropped{s.batch[s.next]}
	}
	s.batch[s.next] = task
	s.next++
	if s.numUsed < s.next {
		s.numUsed = s.next
	}
	if s.next >= s.maxSize {
		s.next = 0
	}
	return
}

// GetBatch returns the current batch maintained by the sliding policy.
func (s *Sliding) GetBatch() Batch {
	return s.batch[:s.numUsed]
}

// MaxSize returns the number of tasks the buffer can hold.
func (s *Sliding) MaxSize() int {
	return s.maxSize
}

// Size returns true if the current batch is empty.
func (s *Sliding) Size() int {
	return s.numUsed
}

// Dropping batching policy maintains a batch of up to MaxSize tasks.
// When full, new tasks will be dropped.
type Dropping struct {
	maxSize int
	batch   Batch
	next    int
}

// NewDropping creates a new dropping batching policy.
func NewDropping(maxSize int) *Dropping {
	return &Dropping{
		maxSize: maxSize,
		batch:   make(Batch, maxSize),
	}
}

// Clears the policy state.
func (d *Dropping) Reset() {
	// We need to create a new slice because the existing one
	// may have already been put onto o channel or referenced
	// in another way.
	d.batch = make(Batch, d.maxSize)
	d.next = 0
}

// AddTasks adds a new task, dropping it if the buffer is full.
func (d *Dropping) AddTask(task task.Task) error {
	if d.next >= d.maxSize {
		return Dropped{task}
	}
	d.batch[d.next] = task
	d.next++
	return nil
}

// GetBatch returns the current batch maintained by the policy.
func (d *Dropping) GetBatch() Batch {
	return d.batch[:d.next]
}

// MaxSize returns the number of tasks the buffer can hold.
func (d *Dropping) MaxSize() int {
	return d.maxSize
}

// Size returns true if the current batch is empty.
func (d *Dropping) Size() int {
	return d.next
}
