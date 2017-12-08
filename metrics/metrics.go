package metrics

import (
	"fmt"
	"github.com/VividCortex/ewma"
	"sync"
	"sync/atomic"
	"time"
)

type Metrics interface {
	SourceName() string
	Begin(delta uint64) Span
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
	AvgTime    ewma.MovingAverage
	mtx        *sync.Mutex
}

func NewBasic(sourceName string) *BasicMetrics {
	return &BasicMetrics{sourceName, 0, 0, 0, ewma.NewMovingAverage(), &sync.Mutex{}}
}

func (metric BasicMetrics) SourceName() string {
	return metric.sourceName
}

func (metric *BasicMetrics) Begin(delta uint64) Span {
	atomic.AddUint64(&metric.Iterations, delta)
	span := BasicSpan{time.Now(), delta, metric}
	return &span
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
	return []string{"iterations", "successes", "failures", "avgt"}
}

func (metric *BasicMetrics) Values() []string {
	metric.mtx.Lock()
	defer metric.mtx.Unlock()
	return []string{
		fmt.Sprintf("%v", metric.Iterations),
		fmt.Sprintf("%v", metric.Successes),
		fmt.Sprintf("%v", metric.Failures),
		fmt.Sprintf("%.2fs", metric.AvgTime.Value()),
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
	if delta == 0 {
		return
	}
	bm := span.metrics.(*BasicMetrics)
	bm.mtx.Lock()
	defer bm.mtx.Unlock()
	span.metrics.EndWithSuccess(delta)
	t := time.Since(span.start).Seconds() / float64(delta)
	for i := uint64(0); i < delta; i++ {
		bm.AvgTime.Add(t)
	}
}

func (span *BasicSpan) Failure(delta uint64) {
	if delta == 0 {
		return
	}
	bm := span.metrics.(*BasicMetrics)
	bm.mtx.Lock()
	defer bm.mtx.Unlock()
	span.metrics.EndWithFailure(delta)
	t := time.Since(span.start).Seconds() / float64(delta)
	for i := uint64(0); i < delta; i++ {
		bm.AvgTime.Add(t)
	}
}

func (span *BasicSpan) Close(err *error) {
	if *err != nil {
		span.Failure(span.delta)
	} else {
		span.Success(span.delta)
	}
}
