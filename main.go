package main

import (
	"github.com/bilus/backpressure/pipeline/examples/fake"

	"github.com/bilus/backpressure/pipeline/runner"
	"golang.org/x/net/context"
	// "runtime/trace"
)

func main() {
	// trace.Start(os.Stdout)
	config := runner.DefaultConfig()
	runner.RunPipeline(context.Background(), config, fake.TaskProducer{500}, fake.BatchConsumer{500})
	// trace.Stop()
}
