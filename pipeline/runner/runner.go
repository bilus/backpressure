package runner

import (
	"github.com/bilus/backpressure/pipeline"
	"github.com/bilus/backpressure/pipeline/batch"
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
	ExecutionTimeLimit          time.Duration
	ShutdownGracePeriod         time.Duration
	ProducerShutdownGracePeriod time.Duration
	DispatchTick                time.Duration
	TaskQueueSize               int
}

func DefaultConfig() Config {
	return Config{
		DispatchTick:                time.Millisecond * 100,
		ExecutionTimeLimit:          0, // No limit.
		ShutdownGracePeriod:         time.Second * 10,
		ProducerShutdownGracePeriod: time.Second * 3,
		TaskQueueSize:               32,
	}
}

func RunPipeline(ctx context.Context, config Config, taskProducer task.Producer, batchConsumer batch.Consumer, metrics pipeline.PipelineMetrics, wg *sync.WaitGroup) {
	pipeline.Run(ctx, config.DispatchTick, config.TaskQueueSize, config.ProducerShutdownGracePeriod, taskProducer, batchConsumer, metrics, wg)
}

// SetupTerminate terminates the pipeline on Ctrl+C.
func SetupTermination(ctx context.Context) context.Context {
	ctx, cancel := context.WithCancel(ctx)
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
	return ctx
}

func WaitToTerminate(ctx context.Context, wg *sync.WaitGroup, gracePeriod time.Duration) {
	<-ctx.Done()
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
