package pipeline

import (
	"fmt"
	"github.com/fatih/color"
	"github.com/nowthisnews/dp-pubsub-archai/metrics"
	"golang.org/x/net/context"
	"log"
	"math/rand"
	"sync"
	"time"
)

// TODO: Globals used across the package, fix this.
var (
	blue    = color.New(color.FgBlue).SprintFunc()
	yellow  = color.New(color.FgYellow).SprintFunc()
	red     = color.New(color.FgRed).SprintFunc()
	magenta = color.New(color.FgMagenta).SprintFunc()
	green   = color.New(color.FgGreen).SprintFunc()
	cyan    = color.New(color.FgCyan).SprintFunc()
)

type PipelineMetrics struct {
	ProducerMetrics   metrics.Metrics
	DispatcherMetrics metrics.Metrics
	ConsumerMetrics   metrics.Metrics
}

func NewPipelineMetrics() PipelineMetrics {
	return PipelineMetrics{
		metrics.NewBasic("producer"),
		metrics.NewBasic("dispatch"),
		metrics.NewBasic("consume"),
	}
}

type FakeTaskProducer struct {
	maxSleep int
}

func (ftp FakeTaskProducer) ProduceTask() Task {
	time.Sleep(time.Millisecond * time.Duration(rand.Intn(ftp.maxSleep)))
	return &FakeTask{rand.Int63()}
}

type FakeBatchConsumer struct {
	maxSleep int
}

type FakeTask struct {
	val int64
}

func (FakeTask) TaskTypeTag() {}

func (fbc FakeBatchConsumer) ConsumeBatch(batch Batch) error {
	log.Println(yellow("<= Writing..."))
	time.Sleep(time.Millisecond * time.Duration(rand.Intn(fbc.maxSleep)))
	taskBatch := make([]string, len(batch))
	for i, task := range batch {
		taskBatch[i] = fmt.Sprintf("%v", *task.(*FakeTask))
	}
	log.Printf(yellow("<= %v"), taskBatch)
	return nil
}

func Run(ctx context.Context, tick time.Duration, wg *sync.WaitGroup) *PipelineMetrics {
	pipelineMetrics := NewPipelineMetrics()
	taskChanSize := 4
	taskCh, taskPermitCh := Produce(ctx, FakeTaskProducer{500}, taskChanSize, pipelineMetrics.ProducerMetrics, wg)
	batchCh, batchPermitCh := Dispatch(ctx, tick, taskChanSize, taskChanSize/2, taskCh, taskPermitCh,
		pipelineMetrics.DispatcherMetrics, wg)
	Consume(ctx, FakeBatchConsumer{1000}, batchCh, batchPermitCh, pipelineMetrics.ConsumerMetrics, wg)
	ReportPeriodically(ctx, time.Second*5, pipelineMetrics.ProducerMetrics, pipelineMetrics.DispatcherMetrics,
		pipelineMetrics.ConsumerMetrics, wg)
	return &pipelineMetrics
}

type Task interface {
	TaskTypeTag()
}
type Batch []Task

// ASK: There are many ways we could improve that. Ideas:
//   + Extend it to provide hints about max batch size.
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
