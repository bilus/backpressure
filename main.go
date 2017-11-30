package main

import (
	"github.com/nowthisnews/dp-pubsub-archai/pipeline"
	// "os"
	// "runtime/trace"
	"log"
	"sync"
	"time"
)

func main() {
	executionTimeLimit := time.Second * 999

	// trace.Start(os.Stdout)
	wg := sync.WaitGroup{}

	pipeline.Run(time.Second*5, &wg)

	// Just to stop when tracing.
	barrierCh := make(chan struct{})
	go func() {
		wg.Wait()
		barrierCh <- struct{}{}
	}()
	select {
	case <-barrierCh:
		log.Println("Pipeline completed")
	case <-time.After(executionTimeLimit):
		log.Println("Timeout waiting for finish")
	}

	// trace.Stop()
}
