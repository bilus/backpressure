package producer

import (
	"github.com/bilus/backpressure/colors"
	"github.com/bilus/backpressure/metrics"
	"github.com/bilus/backpressure/pipeline/permit"
	"github.com/bilus/backpressure/pipeline/task"
	"golang.org/x/net/context"
	"log"
	"sync"
	"time"
)

// ASK:
// How much producer cares about whether data was actually written or not?
//   - Very much. 200 or 201 -> data was written (have to wait for write to finish).
//     Use 'completion ports' using chans = request & response.
//   - Not much. Just put it into the pipeline and respond with 202 Accepted (we'll do our best).

func Run(ctx context.Context, taskProducer task.Producer, taskChanSize int, metrics metrics.Metrics, wg *sync.WaitGroup) (chan task.Task, chan permit.Permit) {
	taskCh := make(chan task.Task, taskChanSize)
	permitCh := make(chan permit.Permit, 1) // Needs to be closed by the caller.

	go func() {
		wg.Add(1)
		defer wg.Done()
		defer close(taskCh)
		for {
			log.Printf(colors.Blue("Obtaining permit..."))
			select {
			case permit := <-permitCh:
				log.Printf(colors.Blue("Got permit: %v"), permit)
				remaining := permit.SizeHint
				for remaining > 0 {
					task := taskProducer.ProduceTask()
					metrics.Begin(1)
					log.Printf(colors.Blue("=> Sending %v"), task)
					select {
					case <-ctx.Done():
						metrics.EndWithFailure(1)
						log.Println(colors.Blue("Exiting producer"))
						return
					case taskCh <- task:
						metrics.EndWithSuccess(1)
						remaining -= 1
						log.Printf(colors.Blue("=> OK, permits remaining: {%v}"), remaining)
					case <-time.After(time.Second * 5):
						metrics.EndWithFailure(1)
						log.Println(colors.Red("=> Timeout in client"))
					}
				}
			case <-ctx.Done():
				log.Println(colors.Blue("Exiting producer"))
				return
			}
		}
	}()

	return taskCh, permitCh
}
