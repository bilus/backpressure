package pipeline

import (
	"context"
	"github.com/bilus/backpressure/metrics"
	"github.com/bilus/backpressure/pipeline/batch"
	"github.com/bilus/backpressure/pipeline/consumer"
	"github.com/bilus/backpressure/pipeline/dispatcher"
	"github.com/bilus/backpressure/pipeline/producer"
	"github.com/bilus/backpressure/pipeline/task"
	"sync"
)

type PipelineMetrics = []metrics.Metrics

func NewMetrics() PipelineMetrics {
	return PipelineMetrics{
		metrics.NewBasic("producer"),
		metrics.NewBasic("dispatch"),
		metrics.NewBasic("consume"),
	}
}

type Config struct {
	Producer   producer.Config
	Dispatcher dispatcher.Config
}

func DefaultConfig() Config {
	return Config{
		Producer:   *producer.DefaultConfig(),
		Dispatcher: *dispatcher.DefaultConfig(),
	}
}

func Go(ctx context.Context, config Config, taskProducer task.Producer, batchConsumer batch.Consumer, pipelineMetrics PipelineMetrics, wg *sync.WaitGroup) {
	taskCh, taskPermitCh := producer.Go(ctx, config.Producer, taskProducer, pipelineMetrics[0], wg)
	batchCh, batchPermitCh := dispatcher.Go(ctx, config.Dispatcher, taskCh, taskPermitCh, pipelineMetrics[1], wg)
	consumer.Run(ctx, batchConsumer, batchCh, batchPermitCh, pipelineMetrics[2], wg)
}
