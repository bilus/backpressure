package pipeline

import (
	"github.com/bilus/backpressure/metrics"
	"github.com/bilus/backpressure/pipeline/consumer"
	"github.com/bilus/backpressure/pipeline/dispatcher"
	"github.com/bilus/backpressure/pipeline/examples/fake"
	"github.com/bilus/backpressure/pipeline/producer"
	"github.com/bilus/backpressure/pipeline/reporter"
	"golang.org/x/net/context"
	"sync"
	"time"
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

func Run(ctx context.Context, tick time.Duration, wg *sync.WaitGroup) *PipelineMetrics {
	pipelineMetrics := NewPipelineMetrics()
	taskChanSize := 4
	taskCh, taskPermitCh := producer.Run(ctx, fake.TaskProducer{500}, taskChanSize, pipelineMetrics.ProducerMetrics, wg)
	batchCh, batchPermitCh := dispatcher.Run(ctx, tick, taskChanSize, taskChanSize/2, taskCh, taskPermitCh,
		pipelineMetrics.DispatcherMetrics, wg)
	consumer.Run(ctx, fake.BatchConsumer{1000}, batchCh, batchPermitCh, pipelineMetrics.ConsumerMetrics, wg)
	reporter.Run(ctx, time.Second*5, pipelineMetrics.ProducerMetrics, pipelineMetrics.DispatcherMetrics,
		pipelineMetrics.ConsumerMetrics, wg)
	return &pipelineMetrics
}
