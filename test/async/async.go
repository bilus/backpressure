package async

import (
	"context"
	"github.com/bilus/backpressure/metrics"
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
