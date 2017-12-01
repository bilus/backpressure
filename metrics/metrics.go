package metrics

import (
	"fmt"
	"sync/atomic"
)

type Metrics interface {
	SourceName() string
	Begin(delta uint64)
	EndWithSuccess(delta uint64)
	EndWithFailure(delta uint64)

	Labels() []string
	Values() []string
}

type BasicMetrics struct {
	sourceName string
	Iterations uint64
	Successes  uint64
	Failures   uint64
}

func NewBasic(sourceName string) Metrics {
	return &BasicMetrics{sourceName, 0, 0, 0}
}

func (metric BasicMetrics) SourceName() string {
	return metric.sourceName
}

func (metric *BasicMetrics) Begin(delta uint64) {
	atomic.AddUint64(&metric.Iterations, delta)
}

func (metric *BasicMetrics) EndWithSuccess(delta uint64) {
	atomic.AddUint64(&metric.Successes, delta)
}

func (metric *BasicMetrics) EndWithFailure(delta uint64) {
	atomic.AddUint64(&metric.Failures, delta)
}

func (metric *BasicMetrics) Labels() []string {
	return []string{"iterations", "successes", "failures"}
}

func (metric *BasicMetrics) Values() []string {
	return []string{
		fmt.Sprintf("%v", metric.Iterations),
		fmt.Sprintf("%v", metric.Successes),
		fmt.Sprintf("%v", metric.Failures),
	}
}
