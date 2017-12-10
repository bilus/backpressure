package producer_test

import (
	"context"
	"errors"
	"github.com/bilus/backpressure/pipeline/permit"
	"github.com/bilus/backpressure/pipeline/producer"
	"github.com/bilus/backpressure/pipeline/task"
	"github.com/bilus/backpressure/test/async"
	. "gopkg.in/check.v1"
	"testing"
	"time"
)

func Test(t *testing.T) { TestingT(t) }

type MySuite struct {
	Config *producer.Config
	async.Suite
}

var _ = Suite(&MySuite{})

func (s *MySuite) SetUpTest(c *C) {
	s.Config = producer.DefaultConfig().WithGracePeriod(0).WithTaskBuffer(32)
	s.Suite.SetUpTest(c)
}

type InfiniteProducer struct {
	lastId int
}

func (p *InfiniteProducer) ProduceTask(ctx context.Context) (task.Task, error) {
	p.lastId++
	return SomeTask{p.lastId}, nil
}

type FailingProducer struct {
	failSinceId int
	lastId      int
}

func (p *FailingProducer) ProduceTask(ctx context.Context) (task.Task, error) {
	p.lastId++
	if p.lastId >= p.failSinceId {
		return nil, errors.New("Oops!")
	}
	return SomeTask{p.lastId}, nil
}

type SomeTask struct {
	Id int
}

func (_ SomeTask) TaskTypeTag() {}

func (s *MySuite) TestDoesNotProduceWithoutPermit(c *C) {
	defer s.WithTimeout(time.Microsecond * 30)()
	producer.Go(s.Ctx, *s.Config, &InfiniteProducer{}, s.Metrics, &s.Wg)
	s.Wg.Wait()
	c.Assert(s.Metrics.Iterations, Equals, uint64(0))
}

func (s *MySuite) TestFullfillsQuotaInPermit(c *C) {
	defer s.WithTimeout(time.Microsecond * 1500)()
	taskCh, permitCh := producer.Go(s.Ctx, *s.Config, &InfiniteProducer{}, s.Metrics, &s.Wg)
	permitCh <- permit.New(4)
	s.Wg.Wait()
	c.Assert(s.Metrics.Iterations, Equals, uint64(4))
	c.Assert(s.Metrics.Successes, Equals, uint64(4))
	c.Assert(s.Metrics.Failures, Equals, uint64(0))
	c.Assert(<-taskCh, Equals, SomeTask{1})
	c.Assert(<-taskCh, Equals, SomeTask{2})
	c.Assert(<-taskCh, Equals, SomeTask{3})
	c.Assert(<-taskCh, Equals, SomeTask{4})
}

func (s *MySuite) TestClosesTaskChanWhenTerminated(c *C) {
	defer s.WithTimeout(time.Microsecond * 500)()
	taskCh, permitCh := producer.Go(s.Ctx, *s.Config, &InfiniteProducer{}, s.Metrics, &s.Wg)
	permitCh <- permit.New(4)
	s.Wg.Wait()
	for i := 0; i < 4; i++ {
		_, ok := <-taskCh
		c.Assert(ok, Equals, true)
	}
	_, ok := <-taskCh
	c.Assert(ok, Equals, false)
}

func (s *MySuite) TestFailuresProducingTasksExceptFirst(c *C) {
	defer s.WithTimeout(time.Microsecond * 100)()
	taskCh, permitCh := producer.Go(s.Ctx, *s.Config.WithTaskBuffer(1), &FailingProducer{failSinceId: 2}, s.Metrics, &s.Wg)
	permitCh <- permit.New(4)
	s.Wg.Wait()
	c.Assert(s.Metrics.Iterations, Equals, uint64(1))
	c.Assert(s.Metrics.Successes, Equals, uint64(1))
	c.Assert(s.Metrics.Failures, Equals, uint64(0))
	c.Assert(<-taskCh, Equals, SomeTask{1})
	_, ok := <-taskCh
	c.Assert(ok, Equals, false)
}

func (s *MySuite) TestFailureAfterProducing(c *C) {
	// This test depends on some internal implementation details. We're setting the taskCh buffer size to 0.
	// As a result, the only place producer will be blocked after the first task is produces is writing
	// to taskCh. After context is cancelled, it will report a failure because a task has been produced
	// but it cannot be queued and is thus lost.
	defer s.WithTimeout(time.Microsecond * 100)()
	taskCh, permitCh := producer.Go(s.Ctx, *s.Config.WithTaskBuffer(0), &InfiniteProducer{}, s.Metrics, &s.Wg)
	permitCh <- permit.New(4)
	s.Wg.Wait()
	c.Assert(s.Metrics.Iterations, Equals, uint64(1))
	c.Assert(s.Metrics.Successes, Equals, uint64(0))
	c.Assert(s.Metrics.Failures, Equals, uint64(1))
	_, ok := <-taskCh
	c.Assert(ok, Equals, false)
}
