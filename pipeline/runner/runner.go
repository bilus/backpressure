package runner

import (
	"github.com/bilus/backpressure/pipeline"
	"github.com/bilus/backpressure/pipeline/batch"
	"github.com/bilus/backpressure/pipeline/reporter"
	"github.com/bilus/backpressure/pipeline/task"
	"golang.org/x/net/context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type Config struct {
	ExecutionTimeLimit  time.Duration
	ShutdownGracePeriod time.Duration
	DispatchTick        time.Duration
	TaskQueueSize       int
}

func DefaultConfig() Config {
	return Config{
		DispatchTick:        time.Millisecond * 100,
		ExecutionTimeLimit:  0, // No limit.
		ShutdownGracePeriod: time.Second * 10,
		TaskQueueSize:       32,
	}
}

func RunPipeline(ctx context.Context, config Config, taskProducer task.Producer, batchConsumer batch.Consumer, wg *sync.WaitGroup) {
	ctx, cancel := context.WithCancel(ctx)
	signalsCh := setupTermination(cancel)

	metrics := pipeline.Run(ctx, config.DispatchTick, config.TaskQueueSize, taskProducer, batchConsumer, wg)

	if config.ExecutionTimeLimit > 0 {
		terminateAfter(config.ExecutionTimeLimit, signalsCh)
	}
	waitToTerminate(ctx, wg, config.ShutdownGracePeriod)

	reporter.ReportMetrics(metrics...)
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

func terminateAfter(t time.Duration, signalsCh chan<- os.Signal) {
	go func() {
		time.Sleep(t)
		signalsCh <- syscall.SIGINT // Simulate Ctrl+C for our tracing.
	}()
}

func waitToTerminate(ctx context.Context, wg *sync.WaitGroup, gracePeriod time.Duration) {
	<-ctx.Done()
	waitWithTimeout(wg, gracePeriod)
}

func waitWithTimeout(wg *sync.WaitGroup, gracePeriod time.Duration) {
	barrierCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(barrierCh)
	}()
	select {
	case <-barrierCh:
		log.Println("Pipeline completed")
	case <-time.After(gracePeriod):
		log.Println("Timeout waiting for finish")
	}
}
