package task

type Task interface {
	TaskTypeTag()
}

type Producer interface {
	ProduceTask() Task
}
