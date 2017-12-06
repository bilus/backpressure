package metrics

import (
	"fmt"
	// "github.com/VividCortex/ewma"
	"sync/atomic"
	"time"
)

type Metrics interface {
	SourceName() string
	Begin(delta uint64) *BasicSpan
	Iterate(delta uint64)
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

func (metric *BasicMetrics) Begin(delta uint64) *BasicSpan {
	atomic.AddUint64(&metric.Iterations, delta)
	return &BasicSpan{time.Now(), delta, metric}
}

func (metric *BasicMetrics) Iterate(delta uint64) {
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

type Span interface {
	Continue(delta uint64)
	Success(delta uint64)
	Failure(delta uint64)
	Close(err *error)
}

type BasicSpan struct {
	start   time.Time
	delta   uint64
	metrics Metrics
}

func (span *BasicSpan) Continue(delta uint64) {
	span.metrics.Iterate(delta)
	atomic.AddUint64(&span.delta, delta)
}

func (span *BasicSpan) Success(delta uint64) {
	span.metrics.EndWithSuccess(delta)
}

func (span *BasicSpan) Failure(delta uint64) {
	span.metrics.EndWithFailure(delta)
}

func (span *BasicSpan) Close(err *error) {
	if *err != nil {
		span.Failure(span.delta)
	} else {
		span.Success(span.delta)
	}
}
