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
	config.DispatchTick = time.Millisecond * 100
	config.TaskQueueSize = 128
	ctx := runner.SetupTermination(context.Background())
	metrics := pipeline.NewMetrics()
	runner.RunPipeline(ctx, config, fake.TaskProducer{1000}, fake.BatchConsumer{500}, metrics, &wg)
	reporter.Run(ctx, time.Second*5, &wg, metrics...)
	runner.WaitToTerminate(ctx, &wg, config.ShutdownGracePeriod)
	// Print the metrics at the end.
	reporter.ReportMetrics(metrics...)
	// trace.Stop()
}
