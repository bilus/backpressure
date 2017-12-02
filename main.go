package main

import (
	"github.com/bilus/backpressure/pipeline/examples/fake"

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
	runner.RunPipeline(context.Background(), config, fake.TaskProducer{1}, fake.BatchConsumer{10}, &wg)
	// trace.Stop()
}
