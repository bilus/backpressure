package pipeline

import (
	"github.com/nowthisnews/dp-pubsub-archai/metrics"
	"github.com/olekukonko/tablewriter"
	"golang.org/x/net/context"
	"os"
	"sync"
	"time"
)

func ReportPeriodically(ctx context.Context, tick time.Duration, producerMetrics, dispatcherMetrics, consumerMetrics metrics.Metrics, wg *sync.WaitGroup) {
	go func() {
		wg.Add(1)
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second * 5):
				ReportMetrics(producerMetrics, dispatcherMetrics, consumerMetrics)
			}
		}
	}()
}

func ReportMetrics(metrics... metrics.Metrics) {
	header := append([]string{"source"},metrics[0].Labels()...)
	rows := make([][]string, len(metrics))
	for i, m := range metrics {
		rows[i] = append([]string{m.SourceName()}, m.Values()...)
	}

	printTable(header, rows)
}

func printTable(header []string, data [][]string) {
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader(header)
	for _, row := range data {
		table.Append(row)
	}
	table.Render()
}
