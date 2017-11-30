package pipeline

import (
	"github.com/nowthisnews/dp-pubsub-archai/metrics"
	"golang.org/x/net/context"
	"log"
	"math/rand"
	"sync"
	"time"
)

// ASK:
// How much producer cares about whether data was actually written or not?
//   - Very much. 200 or 201 -> data was written (have to wait for write to finish).
//     Use 'completion ports' using chans = request & response.
//   - Not much. Just put it into the pipeline and respond with 202 Accepted (we'll do our best).

func Produce(ctx context.Context, taskChanSize int, metrics *metrics.Metrics, wg *sync.WaitGroup) (chan Task, chan Permit) {
	taskCh := make(chan Task, taskChanSize)
	permitCh := make(chan Permit, 1)

	go func() {
		wg.Add(1)
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return

			default:
				log.Printf(blue("Obtaining permit..."))
				permit := <-permitCh
				log.Printf(blue("Got permit: %v"), permit)
				remaining := permit.SizeHint
				for remaining > 0 {
					time.Sleep(time.Millisecond * time.Duration(rand.Intn(500)))
					task := Task(rand.Int63())
					metrics.Begin(1)
					log.Printf(blue("=> Sending %v"), task)
					select {
					case <-ctx.Done():
						return
					case taskCh <- task:
						metrics.EndWithSuccess(1)
						remaining -= 1
						log.Printf(blue("=> OK, permits remaining: {%v}"), remaining)
					case <-time.After(time.Second * 5):
						metrics.EndWithFailure(1)
						log.Println(red("=> Timeout in client"))
					}
				}
			}
		}
	}()

	return taskCh, permitCh
}
