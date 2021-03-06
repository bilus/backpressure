package consumer_test

import (
	"errors"
	"github.com/bilus/backpressure/pipeline/batch"
	"github.com/bilus/backpressure/pipeline/consumer"
	"github.com/bilus/backpressure/pipeline/permit"
	"github.com/bilus/backpressure/test/async"
	"github.com/bilus/backpressure/test/fixtures"
	. "gopkg.in/check.v1"
	"testing"
	"time"
)

func Test(t *testing.T) { TestingT(t) }

type MySuite struct {
	BatchCh  chan batch.Batch
	PermitCh chan permit.Permit
	Config   *consumer.Config
	async.Suite
}

func (s *MySuite) SetUpTest(c *C) {
	s.BatchCh = make(chan batch.Batch, 1)
	s.PermitCh = make(chan permit.Permit, 1)
	s.Config = consumer.DefaultConfig()
	s.Suite.SetUpTest(c)
}

var _ = Suite(&MySuite{})

func (s *MySuite) TestIssuesInitialPermit(c *C) {
	defer s.WithTimeout(time.Microsecond * 200)()
	cc := fixtures.CollectingConsumer{}
	consumer.Go(s.Ctx, *s.Config, &cc, s.BatchCh, s.PermitCh, s.Metrics, &s.Wg)
	permit, ok := <-s.PermitCh
	c.Assert(ok, Equals, true)
	c.Assert(permit.SizeHint, Equals, 1)
	close(s.BatchCh)
	s.Wg.Wait()
	c.Assert(s.Metrics.Iterations, Equals, uint64(0))
}

func (s *MySuite) TestProcessesPermittedBatchUntilClosed(c *C) {
	defer s.WithTimeout(time.Microsecond * 1000)()
	cc := fixtures.CollectingConsumer{}
	consumer.Go(s.Ctx, *s.Config, &cc, s.BatchCh, s.PermitCh, s.Metrics, &s.Wg)
	<-s.PermitCh
	s.BatchCh <- batch.Batch{fixtures.SomeTask{1}, fixtures.SomeTask{2}}
	<-s.PermitCh
	s.BatchCh <- batch.Batch{fixtures.SomeTask{3}, fixtures.SomeTask{4}}
	close(s.BatchCh)
	s.Wg.Wait()
	c.Assert(cc.ConsumedIds, DeepEquals, []int{1, 2, 3, 4})
	c.Assert(s.Metrics.Iterations, Equals, uint64(4))
	c.Assert(s.Metrics.Successes, Equals, uint64(4))
	c.Assert(s.Metrics.Failures, Equals, uint64(0))
}

func (s *MySuite) TestPermitRequiredBeforeBatchAccepted(c *C) {
	s.PermitCh = make(chan permit.Permit)
	s.BatchCh = make(chan batch.Batch)
	defer s.WithTimeout(time.Microsecond * 1300)()
	cc := fixtures.CollectingConsumer{}
	consumer.Go(s.Ctx, *s.Config, &cc, s.BatchCh, s.PermitCh, s.Metrics, &s.Wg)
	// Initial permit was sent when consumer started.
	s.BatchCh <- batch.Batch{fixtures.SomeTask{1}, fixtures.SomeTask{2}}
	// No permit.
	select {
	case s.BatchCh <- batch.Batch{fixtures.SomeTask{3}, fixtures.SomeTask{6}}:
		c.Fatal("Another batch should not be available yet")
	case <-time.After(time.Microsecond * 100):
		// Should fail.
	}
	close(s.BatchCh)
	s.Wg.Wait()
	c.Assert(cc.ConsumedIds, DeepEquals, []int{1, 2})
	c.Assert(s.Metrics.Iterations, Equals, uint64(2))
	c.Assert(s.Metrics.Successes, Equals, uint64(2))
	c.Assert(s.Metrics.Failures, Equals, uint64(0))
}

func (s *MySuite) TestConsumerFailure(c *C) {
	// TODO: Rewrite all tests so they don't rely on the timeout based on a code below.
	cancel := s.WithTimeout(time.Microsecond * 300000)
	defer cancel()
	consumer.Go(s.Ctx, *s.Config, &fixtures.FailingConsumer{errors.New("Ooops!")}, s.BatchCh, s.PermitCh, s.Metrics, &s.Wg)
	<-s.PermitCh
	s.BatchCh <- batch.Batch{fixtures.SomeTask{1}, fixtures.SomeTask{2}}
	close(s.BatchCh)
	cancel()
	s.Wg.Wait()
	c.Assert(s.Metrics.Iterations, Equals, uint64(2))
	c.Assert(s.Metrics.Successes, Equals, uint64(0))
	c.Assert(s.Metrics.Failures, Equals, uint64(2))
}

func (s *MySuite) TestConsumerTerminatesWhileProducing(c *C) {
	defer s.WithTimeout(time.Microsecond * 300000)()
	consumer.Go(s.Ctx, *s.Config, &fixtures.TerminatingConsumer{s.Cancel}, s.BatchCh, s.PermitCh, s.Metrics, &s.Wg)
	<-s.PermitCh
	s.BatchCh <- batch.Batch{fixtures.SomeTask{1}, fixtures.SomeTask{2}}
	close(s.BatchCh)
	s.Wg.Wait()
	c.Assert(s.Metrics.Iterations, Equals, uint64(2))
	c.Assert(s.Metrics.Successes, Equals, uint64(0))
	c.Assert(s.Metrics.Failures, Equals, uint64(2))
}

func (s *MySuite) TestConsumerTerminatesWhileWaitingForBatch(c *C) {
	defer s.WithTimeout(time.Microsecond * 300)()
	consumer.Go(s.Ctx, *s.Config, &fixtures.CollectingConsumer{}, s.BatchCh, s.PermitCh, s.Metrics, &s.Wg)
	<-s.PermitCh
	s.Cancel()
	time.Sleep(time.Microsecond * 10) // Race cond between Cancel and close.
	close(s.BatchCh)
	s.Wg.Wait()
	c.Assert(s.Metrics.Iterations, Equals, uint64(0))
	c.Assert(s.Metrics.Successes, Equals, uint64(0))
	c.Assert(s.Metrics.Failures, Equals, uint64(0))
}

func (s *MySuite) TestConsumerTerminatesWhileSendingPermit(c *C) {
	s.PermitCh = make(chan permit.Permit) // no buffer so consumer blocks
	defer s.WithTimeout(time.Microsecond * 300)()
	consumer.Go(s.Ctx, *s.Config, &fixtures.CollectingConsumer{}, s.BatchCh, s.PermitCh, s.Metrics, &s.Wg)
	s.Cancel()
	close(s.BatchCh)
	s.Wg.Wait()
	c.Assert(s.Metrics.Iterations, Equals, uint64(0))
	c.Assert(s.Metrics.Successes, Equals, uint64(0))
	c.Assert(s.Metrics.Failures, Equals, uint64(0))
}
