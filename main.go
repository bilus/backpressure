package main

import (
	"github.com/nowthisnews/dp-pubsub-archai/pipeline"
	"golang.org/x/net/context"
	// "os"
	// "runtime/trace"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

func main() {
	executionTimeLimit := time.Second * 30
	gracePeriod := time.Second * 15

	// trace.Start(os.Stdout)
	wg := sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	signalsCh := setupTermination(cancel)

	metrics := pipeline.Run(ctx, time.Second*5, &wg)

	// We won't ever wait for this one.
	go func() {
		time.Sleep(executionTimeLimit)
		signalsCh <- syscall.SIGINT // Simulate Ctrl+C for our tracing.
	}()

	waitToTerminate(&wg, gracePeriod)

	pipeline.ReportMetrics(metrics.ProducerMetrics, metrics.DispatcherMetrics, metrics.ConsumerMetrics)
	// trace.Stop()
}

func setupTermination(cancel context.CancelFunc) chan os.Signal {
	signalsCh := make(chan os.Signal, 64)
	signal.Notify(signalsCh, syscall.SIGINT)
	// We won't ever wait for this one.
	go func() {
		select {
		case <-signalsCh:
			log.Println("Terminating...")
			cancel()
		}

	}()
	return signalsCh
}

func waitToTerminate(wg *sync.WaitGroup, gracePeriod time.Duration) {
	// Just to stop when tracing.
	barrierCh := make(chan struct{})
	go func() {
		wg.Wait()
		barrierCh <- struct{}{}
	}()
	select {
	case <-barrierCh:
		log.Println("Pipeline completed")
	case <-time.After(gracePeriod):
		log.Println("Timeout waiting for finish")
	}
}
