package pipeline_test

import (
	"github.com/bilus/backpressure/metrics"
	"github.com/bilus/backpressure/pipeline"
	"github.com/bilus/backpressure/test/async"
	"github.com/bilus/backpressure/test/fixtures"
	. "gopkg.in/check.v1"
	"testing"
	"time"
)

func Test(t *testing.T) { TestingT(t) }

type MySuite struct {
	Config  *pipeline.Config
	Metrics pipeline.PipelineMetrics
	async.Suite
}

func (s *MySuite) SetUpTest(c *C) {
	s.Config = pipeline.DefaultConfig()
	s.Config.Producer.WithGracePeriod(0).WithTaskBuffer(32)
	s.Config.Dispatcher.WithTick(time.Microsecond * 50)
	s.Metrics = pipeline.NewMetrics()
	s.Suite.SetUpTest(c)
}

var _ = Suite(&MySuite{})

func (s *MySuite) TestEverythingIsProduced(c *C) {
	defer s.WithTimeout(time.Microsecond * 1500)()
	cc := fixtures.CollectingConsumer{}
	pipeline.Go(s.Ctx, *s.Config, &fixtures.FiniteProducer{MaxTasks: 4}, &cc, s.Metrics, &s.Wg)
	s.Wg.Wait()

	c.Assert(cc.ConsumedIds, DeepEquals, []int{1, 2, 3, 4})
	for i := 0; i < len(s.Metrics); i++ {
		c.Assert(s.Metrics[i].(*metrics.BasicMetrics).Iterations, Equals, uint64(4))
		c.Assert(s.Metrics[i].(*metrics.BasicMetrics).Successes, Equals, uint64(4))
		c.Assert(s.Metrics[i].(*metrics.BasicMetrics).Failures, Equals, uint64(0))
	}
}
