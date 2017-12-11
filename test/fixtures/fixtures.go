package fixtures

import (
	"context"
	"github.com/bilus/backpressure/pipeline/batch"
	"github.com/bilus/backpressure/pipeline/task"
	"google.golang.org/api/iterator"
)

type SomeTask struct {
	Id int
}

func (_ SomeTask) TaskTypeTag() {}

type InfiniteProducer struct {
	lastId int
}

func (p *InfiniteProducer) ProduceTask(ctx context.Context) (task.Task, error) {
	p.lastId++
	return SomeTask{p.lastId}, nil
}

type FiniteProducer struct {
	MaxTasks int
	lastId   int
}

func (p *FiniteProducer) ProduceTask(ctx context.Context) (task.Task, error) {
	if p.lastId >= p.MaxTasks {
		return nil, iterator.Done
	}
	p.lastId++
	return SomeTask{p.lastId}, nil
}

type FailingProducer struct {
	Err         error
	FailSinceId int
	lastId      int
}

func (p *FailingProducer) ProduceTask(ctx context.Context) (task.Task, error) {
	p.lastId++
	if p.lastId >= p.FailSinceId {
		return nil, p.Err
	}
	return SomeTask{p.lastId}, nil
}

type CollectingConsumer struct {
	ConsumedIds []int
}

func (cc *CollectingConsumer) ConsumeBatch(ctx context.Context, batch batch.Batch) error {
	for _, task := range batch {
		cc.ConsumedIds = append(cc.ConsumedIds, task.(SomeTask).Id)
	}
	return nil
}

type FailingConsumer struct {
	Err error
}

func (fc *FailingConsumer) ConsumeBatch(ctx context.Context, batch batch.Batch) error {
	return fc.Err
}

type TerminatingConsumer struct {
	Cancel context.CancelFunc
}

func (fc *TerminatingConsumer) ConsumeBatch(ctx context.Context, batch batch.Batch) error {
	fc.Cancel()
	<-ctx.Done()
	return ctx.Err()
}
