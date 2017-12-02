package pipeline

import (
	"github.com/bilus/backpressure/metrics"
	"github.com/bilus/backpressure/pipeline/batch"
	"github.com/bilus/backpressure/pipeline/consumer"
	"github.com/bilus/backpressure/pipeline/dispatcher"
	"github.com/bilus/backpressure/pipeline/producer"
	"github.com/bilus/backpressure/pipeline/reporter"
	"github.com/bilus/backpressure/pipeline/task"
	"golang.org/x/net/context"
	"sync"
	"time"
)

type PipelineMetrics = []metrics.Metrics

func Run(ctx context.Context, tick time.Duration, taskQueueSize int, taskProducer task.Producer, batchConsumer batch.Consumer, wg *sync.WaitGroup) PipelineMetrics {
	pipelineMetrics := PipelineMetrics{
		metrics.NewBasic("producer"),
		metrics.NewBasic("dispatch"),
		metrics.NewBasic("consume"),
	}
	taskCh, taskPermitCh := producer.Run(ctx, taskProducer, taskQueueSize, pipelineMetrics[0], wg)
	batchCh, batchPermitCh := dispatcher.Run(ctx, tick, taskQueueSize, taskQueueSize/2, taskCh, taskPermitCh, pipelineMetrics[1], wg)
	consumer.Run(ctx, batchConsumer, batchCh, batchPermitCh, pipelineMetrics[2], wg)
	reporter.Run(ctx, time.Second*5, wg, pipelineMetrics...)
	return pipelineMetrics
}
