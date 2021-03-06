package async

import (
	"context"
	"github.com/bilus/backpressure/metrics"
	"github.com/bilus/backpressure/pipeline/batch"
	"github.com/bilus/backpressure/pipeline/permit"
	check "gopkg.in/check.v1"
	"sync"
	"time"
)

type Suite struct {
	Wg      sync.WaitGroup
	Ctx     context.Context
	Cancel  context.CancelFunc
	Metrics *metrics.BasicMetrics
}

func (s *Suite) SetUpTest(c *check.C) {
	s.Wg = sync.WaitGroup{}
	s.Metrics = metrics.NewBasic("producer")
}

func (s *Suite) TearDownTest(c *check.C) {
	c.Assert(s.Metrics.Iterations, check.Equals, s.Metrics.Successes+s.Metrics.Failures)
}

func (s *Suite) WithTimeout(d time.Duration) context.CancelFunc {
	s.Ctx, s.Cancel = context.WithTimeout(context.Background(), d)
	return s.Cancel
}

func FetchPermit(permitCh <-chan permit.Permit) *permit.Permit {
	select {
	case permit := <-permitCh:
		return &permit
	default:
		return nil
	}
}

func FetchBatch(batchCh <-chan batch.Batch) *batch.Batch {
	select {
	case batch := <-batchCh:
		return &batch
	default:
		return nil
	}
}
