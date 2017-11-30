package metrics

import (
	"sync/atomic"
)

type Metrics struct {
	Iterations uint64
	Successes  uint64
	Failures   uint64
}

func New() Metrics {
	return Metrics{0, 0, 0}
}

func (metric *Metrics) Begin(delta uint64) {
	atomic.AddUint64(&metric.Iterations, delta)
}

func (metric *Metrics) EndWithSuccess(delta uint64) {
	atomic.AddUint64(&metric.Successes, delta)
}

func (metric *Metrics) EndWithFailure(delta uint64) {
	atomic.AddUint64(&metric.Failures, delta)
}
