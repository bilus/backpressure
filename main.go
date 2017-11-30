package main

import (
	"github.com/nowthisnews/dp-pubsub-archai/pipeline"
	// "os"
	// "runtime/trace"
	"time"
)

func main() {
	// trace.Start(os.Stdout)
	pipeline.Run(time.Second * 5)
	// trace.Stop()
}
