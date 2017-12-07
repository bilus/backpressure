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
	async.Suite
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

type FailingConsumer struct{}

func (cc *FailingConsumer) ConsumeBatch(ctx context.Context, batch batch.Batch) error {
	return errors.New("Ooops!")
}

func (s *MySuite) SetUpTest(c *C) {
	s.BatchCh = make(chan batch.Batch, 1)
	s.PermitCh = make(chan permit.Permit)
	s.Suite.SetUpTest(c)
}

func (s *MySuite) TestIssuesInitialPermit(c *C) {
	defer s.WithTimeout(time.Microsecond * 200)()
	cc := CollectingConsumer{}
	consumer.Run(s.Ctx, &cc, s.BatchCh, s.PermitCh, s.Metrics, &s.Wg)
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
	consumer.Run(s.Ctx, &cc, s.BatchCh, s.PermitCh, s.Metrics, &s.Wg)
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
	defer s.WithTimeout(time.Microsecond * 200)()
	cc := CollectingConsumer{}
	consumer.Run(s.Ctx, &cc, s.BatchCh, s.PermitCh, s.Metrics, &s.Wg)
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
	consumer.Run(s.Ctx, &FailingConsumer{}, s.BatchCh, s.PermitCh, s.Metrics, &s.Wg)
	<-s.PermitCh
	s.BatchCh <- batch.Batch{SomeTask{1}, SomeTask{2}}
	close(s.BatchCh)
	s.Wg.Wait()
	c.Assert(s.Metrics.Iterations, Equals, uint64(2))
	c.Assert(s.Metrics.Successes, Equals, uint64(0))
	c.Assert(s.Metrics.Failures, Equals, uint64(2))
}
