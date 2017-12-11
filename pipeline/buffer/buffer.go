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

func New(permitCh chan<- permit.Permit, bp batch.BatchingPolicy, lowWaterMark int) *Buffer {
	b := bucket.New(permitCh, lowWaterMark, bp.MaxSize())
	return &Buffer{
		BatchingPolicy: bp,
		Bucket:         &b}
}

func (b *Buffer) Prefill(ctx context.Context) error {
	return b.Bucket.FillUp(ctx)
}

func (b *Buffer) BufferTask(ctx context.Context, task task.Task) error {
	// Note: We drain bucket regardless whether we manage to buffer the task or not;
	// If a task was taken from the channel, we need to balance it with a permit.
	println("Before:", b.BatchingPolicy.Size())
	err := b.BatchingPolicy.AddTask(task)
	// TODO: What to do when cannot drain?
	b.Bucket.Drain(ctx, 1)
	println(fmt.Sprintf("After: %v %v", b.BatchingPolicy.Size(), err))
	return err
}

func (b *Buffer) Recycle(ctx context.Context) error {
	b.BatchingPolicy.Reset()
	// b.Bucket.FillUp(ctx)
	return nil
}

func (b *Buffer) GetBatch() batch.Batch {
	return b.BatchingPolicy.GetBatch()
}
