package main

import (
	"github.com/nowthisnews/dp-pubsub-archai/pipeline"
	"golang.org/x/net/context"
	// "os"
	// "runtime/trace"
	"log"
	"sync"
	"time"
)

func main() {
	executionTimeLimit := time.Second * 30
	flushTimeLimit := time.Second * 15

	// trace.Start(os.Stdout)
	wg := sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())

	pipeline.Run(ctx, time.Second*5, &wg)

	time.Sleep(executionTimeLimit)
	// We won't ever wait for this one.
	go func() {
		log.Println("Terminating...")
		cancel()
	}()

	// Just to stop when tracing.
	barrierCh := make(chan struct{})
	go func() {
		wg.Wait()
		barrierCh <- struct{}{}
	}()
	select {
	case <-barrierCh:
		log.Println("Pipeline completed")
	case <-time.After(flushTimeLimit):
		log.Println("Timeout waiting for finish")
	}

	// trace.Stop()
}
