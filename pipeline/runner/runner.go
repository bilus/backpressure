package runner

import (
	"context"
	"github.com/bilus/backpressure/pipeline"
	"github.com/bilus/backpressure/pipeline/batch"
	"github.com/bilus/backpressure/pipeline/task"
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
	Pipeline            pipeline.Config
}

func DefaultConfig() Config {
	tgp := time.Second * 30
	pc := pipeline.DefaultConfig()
	pc.Producer.WithGracePeriod(tgp)
	return Config{
		ExecutionTimeLimit:  0, // No limit.
		ShutdownGracePeriod: tgp,
		Pipeline:            pc,
	}
}

func RunPipeline(ctx context.Context, config Config, taskProducer task.Producer, batchConsumer batch.Consumer, metrics pipeline.PipelineMetrics, wg *sync.WaitGroup) {
	pipeline.Go(ctx, config.Pipeline, taskProducer, batchConsumer, metrics, wg)
}

// SetupTerminate terminates the pipeline on Ctrl+C.
func SetupTermination(ctx context.Context) context.Context {
	ctx, cancel := context.WithCancel(ctx)
	signalsCh := make(chan os.Signal, 64)
	signal.Notify(signalsCh, syscall.SIGINT)
	// We won't ever wait for this one.
	go func() {
		<-signalsCh
		log.Println("Terminating...")
		cancel()
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
