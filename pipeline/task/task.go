package task

import (
	"context"
)

type Task interface {
	TaskTypeTag()
}

type Producer interface {
	ProduceTask(ctx context.Context) (Task, error)
}
