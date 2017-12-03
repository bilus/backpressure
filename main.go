package main

import (
	"github.com/bilus/backpressure/pipeline/examples/fake"
	"github.com/bilus/backpressure/pipeline/reporter"
	"github.com/bilus/backpressure/pipeline/runner"
	"golang.org/x/net/context"
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
	metrics := runner.RunPipeline(ctx, config, fake.TaskProducer{1}, fake.BatchConsumer{10}, &wg)
	runner.WaitToTerminate(ctx, &wg, config.ShutdownGracePeriod)
	// Print the metrics at the end.
	reporter.ReportMetrics(metrics...)
	// trace.Stop()
}
