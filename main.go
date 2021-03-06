package main

import (
	"context"
	"github.com/bilus/backpressure/pipeline"
	"github.com/bilus/backpressure/pipeline/examples/fake"
	"github.com/bilus/backpressure/pipeline/reporter"
	"github.com/bilus/backpressure/pipeline/runner"
	"sync"
	"time"
	// "runtime/trace"
)

func main() {
	// trace.Start(os.Stdout)
	wg := sync.WaitGroup{}
	config := runner.DefaultConfig()
	// config.Pipeline.Dispatcher.WithTick(time.Millisecond * 100)
	// config.Pipeline.Producer.WithTaskBuffer(10)
	config.Pipeline.Dispatcher.WithDroppingPolicy(300)
	ctx := runner.SetupTermination(context.Background())
	metrics := pipeline.NewMetrics()
	runner.RunPipeline(ctx, config, fake.TaskProducer{10}, fake.BatchConsumer{1000}, metrics, &wg)
	reporter.Run(ctx, time.Second*5, &wg, metrics...)
	runner.WaitToTerminate(ctx, &wg, config.ShutdownGracePeriod)
	// Print the metrics at the end.
	reporter.ReportMetrics(metrics...)
	// trace.Stop()
}
