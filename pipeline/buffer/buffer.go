package buffer

import (
	"context"
	"fmt"
	"github.com/bilus/backpressure/pipeline/batch"
	"github.com/bilus/backpressure/pipeline/bucket"
	"github.com/bilus/backpressure/pipeline/permit"
	"github.com/bilus/backpressure/pipeline/task"
)

type Buffer struct {
	batch.BatchingPolicy
	*bucket.Bucket
}

func New(permitCh chan<- permit.Permit, bp batch.BatchingPolicy) *Buffer {
	// Make sure we have enough space for 1.5 batch size because of the 0.5 low water mark.
	highWaterMark := int(float64(bp.MaxSize()) / 1.5)
	lowWaterMark := highWaterMark / 2
	b := bucket.New(permitCh, lowWaterMark, highWaterMark)
	return &Buffer{
		BatchingPolicy: bp,
		Bucket:         &b}
}

func (b *Buffer) Prefill(ctx context.Context) error {
	b.checkInvariants()
	err := b.Bucket.FillUp(ctx, b.availablePermits())
	b.checkInvariants()
	return err
}

func (b *Buffer) BufferTask(ctx context.Context, task task.Task) error {
	b.checkInvariants()
	// Note: We drain bucket regardless whether we manage to buffer the task or not;
	// If a task was taken from the channel, we need to balance it with a permit.
	err := b.BatchingPolicy.AddTask(task)
	// TODO: What to do when cannot fill up or drain?
	b.Bucket.Drain(ctx, 1)
	b.Bucket.FillUp(ctx, b.availablePermits())
	b.checkInvariants()
	return err
}

func (b *Buffer) Flush(ctx context.Context, batchCh chan<- batch.Batch) (int, error) {
	b.checkInvariants()
	currentBatch := b.GetBatch()
	if len(currentBatch) > 0 {
		batchCh <- currentBatch
	}
	b.BatchingPolicy.Reset()
	b.checkInvariants()
	b.Bucket.FillUp(ctx, b.availablePermits())
	return len(currentBatch), nil
}

func (b *Buffer) GetBatch() batch.Batch {
	return b.BatchingPolicy.GetBatch()
}

func (b *Buffer) availablePermits() int {
	// Tasks in batch + tasks requested.
	used := b.BatchingPolicy.Size() + b.Bucket.WaterLevel
	return b.BatchingPolicy.MaxSize() - used
}

func (b *Buffer) checkInvariants() {
	if b.availablePermits() < 0 {
		panic(fmt.Sprintf("Too many permits issued size=%v wl=%v max=%v", b.Size(), b.Bucket.WaterLevel, b.MaxSize()))
	}
}
