package dispatcher_test

import (
	"github.com/bilus/backpressure/pipeline/batch"
	"github.com/bilus/backpressure/pipeline/dispatcher"
	"github.com/bilus/backpressure/pipeline/permit"
	"github.com/bilus/backpressure/pipeline/task"
	"github.com/bilus/backpressure/test/async"
	"github.com/bilus/backpressure/test/fixtures"
	. "gopkg.in/check.v1"
	"log"
	"testing"
	"time"
)

func Test(t *testing.T) { TestingT(t) }

type MySuite struct {
	Config        *dispatcher.Config
	BatchCh       chan batch.Batch
	BatchPermitCh chan permit.Permit
	TaskCh        chan task.Task
	TaskPermitCh  chan permit.Permit
	async.Suite
}

var _ = Suite(&MySuite{})

func (s *MySuite) SetUpTest(c *C) {
	s.Config = dispatcher.DefaultConfig().WithDroppingPolicy(30)
	s.BatchCh = make(chan batch.Batch, 1)
	s.BatchPermitCh = make(chan permit.Permit, 1)
	s.TaskCh = make(chan task.Task, 1)
	s.TaskPermitCh = make(chan permit.Permit, 1)
	log.Println("SetUpTest", c, len(s.TaskPermitCh))
	s.Suite.SetUpTest(c)
}

func (s *MySuite) TestIssuesPermit(c *C) {
	defer s.WithTimeout(time.Microsecond * 1000)()
	s.BatchCh, s.BatchPermitCh = dispatcher.Go(s.Ctx, *s.Config, s.TaskCh, s.TaskPermitCh, s.Metrics, &s.Wg)
	close(s.TaskCh)
	s.Wg.Wait()
	taskPermit := <-s.TaskPermitCh
	c.Assert(taskPermit, Equals, permit.Permit{20})
	c.Assert(s.Metrics.Iterations, Equals, uint64(0))
	c.Assert(s.Metrics.Successes, Equals, uint64(0))
	c.Assert(s.Metrics.Failures, Equals, uint64(0))
	_, ok := <-s.BatchCh
	c.Assert(ok, Equals, false)
}

func (s *MySuite) TestBuffersTasksInBatches(c *C) {
	defer s.WithTimeout(time.Microsecond * 1500)()
	s.BatchCh, s.BatchPermitCh = dispatcher.Go(s.Ctx, *s.Config.WithTick(time.Microsecond * 500), s.TaskCh, s.TaskPermitCh, s.Metrics, &s.Wg)
	<-s.TaskPermitCh
	for i := 0; i < 5; i++ {
		s.TaskCh <- fixtures.SomeTask{i}
	}
	close(s.TaskCh)
	s.BatchPermitCh <- permit.Permit{1} // Allow 1 batch.
	time.Sleep(time.Microsecond * 500)  // Wait for flush
	batch := <-s.BatchCh
	c.Assert(len(batch), Equals, 5)
	s.Wg.Wait()
	c.Assert(s.Metrics.Iterations, Equals, uint64(5))
	c.Assert(s.Metrics.Successes, Equals, uint64(5))
	c.Assert(s.Metrics.Failures, Equals, uint64(0))
	c.Assert(len(batch), Equals, 5)
	for i := 0; i < 5; i++ {
		c.Assert(batch[i], Equals, fixtures.SomeTask{i})
	}
}

func (s *MySuite) TestFlushesPeriodically(c *C) {
	defer s.WithTimeout(time.Microsecond * 6000)()
	s.BatchCh, s.BatchPermitCh = dispatcher.Go(s.Ctx, *s.Config.WithTick(time.Microsecond * 5000), s.TaskCh, s.TaskPermitCh, s.Metrics, &s.Wg)
	<-s.TaskPermitCh
	s.BatchPermitCh <- permit.Permit{1} // Allow 1 batch.
	s.TaskCh <- fixtures.SomeTask{1}
	close(s.TaskCh)
	c.Assert(async.FetchBatch(s.BatchCh), IsNil) // Not flushed yet.
	time.Sleep(time.Microsecond * 5000)
	batch := <-s.BatchCh
	c.Assert(len(batch), Equals, 1)
	s.Wg.Wait()
	c.Assert(s.Metrics.Iterations, Equals, uint64(1))
	c.Assert(s.Metrics.Successes, Equals, uint64(1))
	c.Assert(s.Metrics.Failures, Equals, uint64(0))
}

func (s *MySuite) TestIssuesPermitBelowWateMark(c *C) {
	defer s.WithTimeout(time.Microsecond * 1500)()
	s.BatchCh, s.BatchPermitCh = dispatcher.Go(s.Ctx, *s.Config.WithTick(time.Microsecond * 500), s.TaskCh, s.TaskPermitCh, s.Metrics, &s.Wg)
	<-s.TaskPermitCh
	s.BatchPermitCh <- permit.Permit{1} // Allow 1 batch.
	c.Assert(async.FetchPermit(s.TaskPermitCh), IsNil)
	for i := 0; i < 15; i++ {
		s.TaskCh <- fixtures.SomeTask{i}
	}
	close(s.TaskCh)
	<-s.BatchCh // Wait for flush
	permit := <-s.TaskPermitCh
	c.Assert(permit.SizeHint, Equals, 10)
	s.Wg.Wait()
	c.Assert(s.Metrics.Iterations, Equals, uint64(15))
	c.Assert(s.Metrics.Successes, Equals, uint64(15))
	c.Assert(s.Metrics.Failures, Equals, uint64(0))
}

func (s *MySuite) TestSlidingBatchingPolicy(c *C) {
	defer s.WithTimeout(time.Microsecond * 1500)()
	s.BatchCh, s.BatchPermitCh = dispatcher.Go(s.Ctx, *s.Config.WithSlidingPolicy(6).WithTick(time.Microsecond * 500), s.TaskCh, s.TaskPermitCh, s.Metrics, &s.Wg)
	<-s.TaskPermitCh
	for i := 0; i < 8; i++ {
		s.TaskCh <- fixtures.SomeTask{i}
	}
	close(s.TaskCh)
	s.BatchPermitCh <- permit.Permit{1} // Allow 1 batch.
	time.Sleep(time.Microsecond * 500)  // Wait for flush
	batch := <-s.BatchCh
	c.Assert(len(batch), Equals, 6)
	s.Wg.Wait()
	c.Assert(s.Metrics.Iterations, Equals, uint64(8))
	c.Assert(s.Metrics.Successes, Equals, uint64(6))
	c.Assert(s.Metrics.Failures, Equals, uint64(2))
	c.Assert(batch[0], Equals, fixtures.SomeTask{6})
	c.Assert(batch[1], Equals, fixtures.SomeTask{7})
	c.Assert(batch[2], Equals, fixtures.SomeTask{2})
	c.Assert(batch[3], Equals, fixtures.SomeTask{3})
	c.Assert(batch[4], Equals, fixtures.SomeTask{4})
	c.Assert(batch[5], Equals, fixtures.SomeTask{5})
}

func (s *MySuite) TestDroppingBatchingPolicy(c *C) {
	defer s.WithTimeout(time.Microsecond * 2500)()
	s.BatchCh, s.BatchPermitCh = dispatcher.Go(s.Ctx, *s.Config.WithDroppingPolicy(6).WithTick(time.Microsecond * 500), s.TaskCh, s.TaskPermitCh, s.Metrics, &s.Wg)
	<-s.TaskPermitCh
	for i := 0; i < 8; i++ {
		s.TaskCh <- fixtures.SomeTask{i}
	}
	close(s.TaskCh)
	s.BatchPermitCh <- permit.Permit{1} // Allow 1 batch.
	time.Sleep(time.Microsecond * 500)  // Wait for flush
	batch := <-s.BatchCh
	c.Assert(len(batch), Equals, 6)
	s.Wg.Wait()
	c.Assert(s.Metrics.Iterations, Equals, uint64(8))
	c.Assert(s.Metrics.Successes, Equals, uint64(6))
	c.Assert(s.Metrics.Failures, Equals, uint64(2))
	c.Assert(batch[0], Equals, fixtures.SomeTask{0})
	c.Assert(batch[1], Equals, fixtures.SomeTask{1})
	c.Assert(batch[2], Equals, fixtures.SomeTask{2})
	c.Assert(batch[3], Equals, fixtures.SomeTask{3})
	c.Assert(batch[4], Equals, fixtures.SomeTask{4})
	c.Assert(batch[5], Equals, fixtures.SomeTask{5})
}
