package consumer_test

import (
	"context"
	"errors"
	"github.com/bilus/backpressure/pipeline/batch"
	"github.com/bilus/backpressure/pipeline/consumer"
	"github.com/bilus/backpressure/pipeline/permit"
	"github.com/bilus/backpressure/test/async"
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
	s.PermitCh = make(chan permit.Permit)
	s.Config = consumer.DefaultConfig()
	s.Suite.SetUpTest(c)
}

var _ = Suite(&MySuite{})

type SomeTask struct {
	Id int
}

func (_ SomeTask) TaskTypeTag() {}

type CollectingConsumer struct {
	ConsumedIds []int
}

func (cc *CollectingConsumer) ConsumeBatch(ctx context.Context, batch batch.Batch) error {
	for _, task := range batch {
		cc.ConsumedIds = append(cc.ConsumedIds, task.(SomeTask).Id)
	}
	return nil
}

type FailingConsumer struct {
	Err error
}

func (fc *FailingConsumer) ConsumeBatch(ctx context.Context, batch batch.Batch) error {
	return fc.Err
}

type TerminatingConsumer struct {
	Cancel context.CancelFunc
}

func (fc *TerminatingConsumer) ConsumeBatch(ctx context.Context, batch batch.Batch) error {
	fc.Cancel()
	<-ctx.Done()
	return ctx.Err()
}

func (s *MySuite) TestIssuesInitialPermit(c *C) {
	defer s.WithTimeout(time.Microsecond * 200)()
	cc := CollectingConsumer{}
	consumer.Go(s.Ctx, *s.Config, &cc, s.BatchCh, s.PermitCh, s.Metrics, &s.Wg)
	permit, ok := <-s.PermitCh
	c.Assert(ok, Equals, true)
	c.Assert(permit.SizeHint, Equals, 1)
	close(s.BatchCh)
	s.Wg.Wait()
	c.Assert(s.Metrics.Iterations, Equals, uint64(0))
}

func (s *MySuite) TestProcessesPermittedBatchUntilClosed(c *C) {
	defer s.WithTimeout(time.Microsecond * 200)()
	cc := CollectingConsumer{}
	consumer.Go(s.Ctx, *s.Config, &cc, s.BatchCh, s.PermitCh, s.Metrics, &s.Wg)
	<-s.PermitCh
	s.BatchCh <- batch.Batch{SomeTask{1}, SomeTask{2}}
	<-s.PermitCh
	s.BatchCh <- batch.Batch{SomeTask{3}, SomeTask{4}}
	close(s.BatchCh)
	s.Wg.Wait()
	c.Assert(cc.ConsumedIds, DeepEquals, []int{1, 2, 3, 4})
	c.Assert(s.Metrics.Iterations, Equals, uint64(4))
	c.Assert(s.Metrics.Successes, Equals, uint64(4))
	c.Assert(s.Metrics.Failures, Equals, uint64(0))
}

func (s *MySuite) TestPermitRequiredBeforeBatchAccepted(c *C) {
	defer s.WithTimeout(time.Microsecond * 300)()
	cc := CollectingConsumer{}
	consumer.Go(s.Ctx, *s.Config, &cc, s.BatchCh, s.PermitCh, s.Metrics, &s.Wg)
	<-s.PermitCh
	s.BatchCh <- batch.Batch{SomeTask{1}, SomeTask{2}}
	s.BatchCh <- batch.Batch{SomeTask{3}, SomeTask{4}}
	// No permit.
	select {
	case s.BatchCh <- batch.Batch{SomeTask{5}, SomeTask{6}}:
		c.Fatal("Another batch should not be available yet")
	case <-time.After(time.Microsecond * 100):
		// Should fail.
	}
	close(s.BatchCh)
	s.Wg.Wait()
	c.Assert(cc.ConsumedIds, DeepEquals, []int{1, 2, 3, 4})
	c.Assert(s.Metrics.Iterations, Equals, uint64(4))
	c.Assert(s.Metrics.Successes, Equals, uint64(4))
	c.Assert(s.Metrics.Failures, Equals, uint64(0))
}

func (s *MySuite) TestConsumerFailure(c *C) {
	defer s.WithTimeout(time.Microsecond * 300)()
	consumer.Go(s.Ctx, *s.Config, &FailingConsumer{errors.New("Ooops!")}, s.BatchCh, s.PermitCh, s.Metrics, &s.Wg)
	<-s.PermitCh
	s.BatchCh <- batch.Batch{SomeTask{1}, SomeTask{2}}
	close(s.BatchCh)
	s.Wg.Wait()
	c.Assert(s.Metrics.Iterations, Equals, uint64(2))
	c.Assert(s.Metrics.Successes, Equals, uint64(0))
	c.Assert(s.Metrics.Failures, Equals, uint64(2))
}

func (s *MySuite) TestConsumerTerminatesWhileProducing(c *C) {
	defer s.WithTimeout(time.Microsecond * 300)()
	consumer.Go(s.Ctx, *s.Config, &TerminatingConsumer{s.Cancel}, s.BatchCh, s.PermitCh, s.Metrics, &s.Wg)
	<-s.PermitCh
	s.BatchCh <- batch.Batch{SomeTask{1}, SomeTask{2}}
	close(s.BatchCh)
	s.Wg.Wait()
	c.Assert(s.Metrics.Iterations, Equals, uint64(2))
	c.Assert(s.Metrics.Successes, Equals, uint64(0))
	c.Assert(s.Metrics.Failures, Equals, uint64(2))
}

func (s *MySuite) TestConsumerTerminatesWhileWaitingForBatch(c *C) {
	defer s.WithTimeout(time.Microsecond * 300)()
	consumer.Go(s.Ctx, *s.Config, &CollectingConsumer{}, s.BatchCh, s.PermitCh, s.Metrics, &s.Wg)
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
	consumer.Go(s.Ctx, *s.Config, &CollectingConsumer{}, s.BatchCh, s.PermitCh, s.Metrics, &s.Wg)
	s.Cancel()
	close(s.BatchCh)
	s.Wg.Wait()
	c.Assert(s.Metrics.Iterations, Equals, uint64(0))
	c.Assert(s.Metrics.Successes, Equals, uint64(0))
	c.Assert(s.Metrics.Failures, Equals, uint64(0))
}
