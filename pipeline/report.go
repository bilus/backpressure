package pipeline

import (
	"fmt"
	"github.com/nowthisnews/dp-pubsub-archai/metrics"
	"github.com/olekukonko/tablewriter"
	"golang.org/x/net/context"
	"os"
	"sync"
	"time"
)

func ReportPeriodically(ctx context.Context, tick time.Duration, producerMetrics, dispatcherMetrics, consumerMetrics *metrics.Metrics, wg *sync.WaitGroup) {
	go func() {
		wg.Add(1)
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second * 5):
				ReportMetrics(*producerMetrics, *dispatcherMetrics, *consumerMetrics)
			}
		}
	}()
}

func ReportMetrics(producerMetrics, dispatcherMetrics, consumerMetrics metrics.Metrics) {
	printTable([]string{"source", "iterations", "successes", "failures"},
		[][]string{
			[]string{"producer", fmt.Sprintf("%v", producerMetrics.Iterations), fmt.Sprintf("%v", producerMetrics.Successes), fmt.Sprintf("%v", producerMetrics.Failures)},
			[]string{"dispatch", fmt.Sprintf("%v", dispatcherMetrics.Iterations), fmt.Sprintf("%v", dispatcherMetrics.Successes), fmt.Sprintf("%v", dispatcherMetrics.Failures)},
			[]string{"consumer", fmt.Sprintf("%v", consumerMetrics.Iterations), fmt.Sprintf("%v", consumerMetrics.Successes), fmt.Sprintf("%v", consumerMetrics.Failures)},
		},
	)
}

func printTable(header []string, data [][]string) {
	table := tablewriter.NewWriter(os.Stderr) // TODO: Write to log.
	table.SetHeader(header)
	for _, row := range data {
		table.Append(row)
	}
	table.Render()
}
