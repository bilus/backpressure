package reporter

import (
	"context"
	"fmt"
	"github.com/bilus/backpressure/metrics"
	"github.com/olekukonko/tablewriter"
	"os"
	"sync"
	"time"
)

func Run(ctx context.Context, tick time.Duration, wg *sync.WaitGroup, metrics ...metrics.Metrics) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second * 5):
				ReportMetrics(metrics...)
			}
		}
	}()
}

func ReportMetrics(pipelineMetrics ...metrics.Metrics) {
	header := append([]string{"source"}, pipelineMetrics[0].Labels()...)
	rows := make([][]string, len(pipelineMetrics))
	for i, m := range pipelineMetrics {
		values := make([]string, len(m.Values()))
		for j := 0; j < len(m.Values()); j++ {
			values[j] = fmt.Sprintf("%.f", m.Values()[j])
		}
		rows[i] = append([]string{m.SourceName()}, values...)
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
