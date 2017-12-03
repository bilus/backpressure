package task

import (
	"golang.org/x/net/context"
)

type Task interface {
	TaskTypeTag()
}

type DoneTriggerable interface {
	error
	Done() bool
}

type Error struct {
	Message         string
	TriggeredByDone bool
}

func (err Error) Error() string {
	return err.Message
}

func (err Error) Done() bool {
	return err.TriggeredByDone
}

type Producer interface {
	ProduceTask(ctx context.Context) (Task, error)
}
