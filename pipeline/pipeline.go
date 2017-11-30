package pipeline

import (
	"github.com/fatih/color"
	"github.com/nowthisnews/dp-pubsub-archai/metrics"
	"golang.org/x/net/context"
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

func Run(ctx context.Context, tick time.Duration, wg *sync.WaitGroup) {
	producerMetrics := metrics.New()
	taskCh, taskPermitCh := Produce(ctx, &producerMetrics, wg)
	dispatcherMetrics := metrics.New()
	batchCh, batchPermitCh := Dispatch(ctx, tick, taskCh, taskPermitCh, &dispatcherMetrics, wg)
	consumerMetrics := metrics.New()
	Consume(ctx, batchCh, batchPermitCh, &consumerMetrics, wg)
	ReportPeriodically(ctx, time.Second*5, &producerMetrics, &dispatcherMetrics, &consumerMetrics, wg)
}

type Task int64
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
