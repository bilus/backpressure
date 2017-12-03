package fake

import (
	"fmt"
	"github.com/bilus/backpressure/colors"
	"github.com/bilus/backpressure/pipeline/batch"
	"github.com/bilus/backpressure/pipeline/task"
	"golang.org/x/net/context"
	"log"
	"math/rand"
	"time"
)

type TaskProducer struct {
	MaxSleep int
}

func (ftp TaskProducer) ProduceTask(_ctx context.Context) (task.Task, error) {
	time.Sleep(time.Millisecond * time.Duration(rand.Intn(ftp.MaxSleep)))
	return &Task{rand.Int63()}, nil
}

type BatchConsumer struct {
	MaxSleep int
}

type Task struct {
	val int64
}

func (Task) TaskTypeTag() {}

func (fbc BatchConsumer) ConsumeBatch(_ctx context.Context, batch batch.Batch) error {
	log.Println(colors.Yellow("<= Writing..."))
	time.Sleep(time.Millisecond * time.Duration(rand.Intn(fbc.MaxSleep)))
	taskBatch := make([]string, len(batch))
	for i, task := range batch {
		taskBatch[i] = fmt.Sprintf("%v", *task.(*Task))
	}
	log.Printf(colors.Yellow("<= %v"), taskBatch)
	return nil
}
