package main

import (
	"fmt"
	"github.com/bilus/backpressure/colors"
	"github.com/bilus/backpressure/httputil"
	"github.com/bilus/backpressure/pipeline/batch"
	"github.com/bilus/backpressure/pipeline/runner"
	"github.com/bilus/backpressure/pipeline/task"
	"golang.org/x/net/context"
	"io/ioutil"
	"log"
	"net/http"
	"sync"
	"time"
)

// Usage url -X POST localhost:3000 --data "Hello world"
func Run(port int) {
	pushCh := make(chan Task)
	wg := sync.WaitGroup{}
	server, err := httputil.ListenAndServeWithClose(fmt.Sprintf(":%v", port), pushHandler(pushCh), &wg)
	if err != nil {
		log.Println(colors.Red(err))
		return
	}
	config := runner.DefaultConfig()
	runner.RunPipeline(context.Background(), config, TaskProducer{pushCh}, BatchConsumer{}, &wg)
	server.Close()
}

type AppHandler func(http.ResponseWriter, *http.Request) (int, error)

func (fn AppHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	status, err := fn(w, r)
	if err != nil {
		http.Error(w, fmt.Sprintf("Internal server error: %v", err), http.StatusInternalServerError)
		return
	}

	switch {
	case status >= 0 && status < 300:
		w.Write([]byte("OK"))
	default:
		http.Error(w, http.StatusText(status), status)
	}
}

func pushHandler(pushCh chan<- Task) AppHandler {
	return func(w http.ResponseWriter, r *http.Request) (int, error) {
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			return http.StatusBadRequest, err
		}
		select {
		case pushCh <- Task{string(body)}:
			return http.StatusOK, nil
		case <-time.After(time.Second * 5):
			return http.StatusTooManyRequests, nil
		}
	}
}

type TaskProducer struct {
	pushCh <-chan Task
}

func (ftp TaskProducer) ProduceTask() task.Task {
	task := <-ftp.pushCh // TODO: No way to report errors from ProduceTask, ugh!
	return &task
}

type BatchConsumer struct {
	MaxSleep int
}

type Task struct {
	message string
}

func (Task) TaskTypeTag() {}

func (fbc BatchConsumer) ConsumeBatch(batch batch.Batch) error {
	log.Println(colors.Yellow("<= Writing"))
	taskBatch := make([]string, len(batch))
	for i, task := range batch {
		taskBatch[i] = fmt.Sprintf("%v", task.(*Task).message)
	}
	log.Printf(colors.Yellow("<= %v"), taskBatch)
	return nil
}

func main() {
	Run(3000)
}
