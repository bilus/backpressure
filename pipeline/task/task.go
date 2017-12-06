package task

import (
	"golang.org/x/net/context"
)

type Task interface {
	TaskTypeTag()
}

type Producer interface {
	ProduceTask(ctx context.Context) (Task, error)
}
