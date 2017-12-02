package main

import (
	"github.com/bilus/backpressure/pipeline/examples/fake"

	"github.com/bilus/backpressure/pipeline/runner"
	"golang.org/x/net/context"
	"time"
	// "runtime/trace"
)

func main() {
	// trace.Start(os.Stdout)
	config := runner.DefaultConfig()
	config.DispatchTick = time.Millisecond * 100
	config.TaskQueueSize = 128
	runner.RunPipeline(context.Background(), config, fake.TaskProducer{1}, fake.BatchConsumer{10})
	// trace.Stop()
}
