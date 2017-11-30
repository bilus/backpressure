package pipeline

import (
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

type Metrics struct {
	ProducerMetrics   metrics.Metrics
	DispatcherMetrics metrics.Metrics
	ConsumerMetrics   metrics.Metrics
}

type FakeTaskProducer struct {
	maxSleep int
}

func (ftp FakeTaskProducer) ProduceTask() *Task {
	time.Sleep(time.Millisecond * time.Duration(rand.Intn(ftp.maxSleep)))
	return &Task{rand.Int63()}
}

type FakeBatchConsumer struct {
	maxSleep int
}

func (fbc FakeBatchConsumer) ConsumeBatch(batch Batch) error {
	log.Println(yellow("<= Writing..."))
	time.Sleep(time.Millisecond * time.Duration(rand.Intn(fbc.maxSleep)))
	log.Printf(yellow("<= %v"), batch)
	return nil
}

func Run(ctx context.Context, tick time.Duration, wg *sync.WaitGroup) *Metrics {
	pipelineMetrics := Metrics{}
	taskChanSize := 4
	taskCh, taskPermitCh := Produce(ctx, FakeTaskProducer{500}, taskChanSize, &pipelineMetrics.ProducerMetrics, wg)
	batchCh, batchPermitCh := Dispatch(ctx, tick, taskChanSize, taskChanSize/2, taskCh, taskPermitCh,
		&pipelineMetrics.DispatcherMetrics, wg)
	Consume(ctx, FakeBatchConsumer{1000}, batchCh, batchPermitCh, &pipelineMetrics.ConsumerMetrics, wg)
	ReportPeriodically(ctx, time.Second*5, &pipelineMetrics.ProducerMetrics, &pipelineMetrics.DispatcherMetrics,
		&pipelineMetrics.ConsumerMetrics, wg)
	return &pipelineMetrics
}

type Task struct {
	val int64
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
