package pipeline

import (
	"github.com/bilus/backpressure/metrics"
	"github.com/bilus/backpressure/pipeline/batch"
	"github.com/bilus/backpressure/pipeline/consumer"
	"github.com/bilus/backpressure/pipeline/dispatcher"
	"github.com/bilus/backpressure/pipeline/producer"
	"github.com/bilus/backpressure/pipeline/task"
	"golang.org/x/net/context"
	"sync"
	"time"
)

type PipelineMetrics = []metrics.Metrics

func NewMetrics() PipelineMetrics {
	return PipelineMetrics{
		metrics.NewBasic("producer"),
		metrics.NewBasic("dispatch"),
		metrics.NewBasic("consume"),
	}
}

func Run(ctx context.Context, tick time.Duration, taskQueueSize int, shutdownGracePeriod time.Duration, taskProducer task.Producer, batchConsumer batch.Consumer, pipelineMetrics PipelineMetrics, wg *sync.WaitGroup) {
	taskCh, taskPermitCh := producer.Run(ctx, taskProducer, taskQueueSize, shutdownGracePeriod, pipelineMetrics[0], wg)
	batchCh, batchPermitCh := dispatcher.Run(ctx, tick, taskQueueSize, taskQueueSize/2, taskCh, taskPermitCh, pipelineMetrics[1], wg)
	consumer.Run(ctx, batchConsumer, batchCh, batchPermitCh, pipelineMetrics[2], wg)
}
