package buffer_test

import (
	"context"
	"github.com/bilus/backpressure/pipeline/batch"
	"github.com/bilus/backpressure/pipeline/buffer"
	"github.com/bilus/backpressure/pipeline/permit"
	"github.com/bilus/backpressure/test/async"
	"github.com/bilus/backpressure/test/fixtures"
	. "gopkg.in/check.v1"
	"testing"
	"time"
)

var _ = time.Now

func Test(t *testing.T) { TestingT(t) }

type MySuite struct {
	MaxSize    int
	PermitChan chan permit.Permit
	Buffer     *buffer.Buffer
	Ctx        context.Context
}

var _ = Suite(&MySuite{})

func (s *MySuite) SetUpTest(c *C) {
	s.MaxSize = 6
	s.Ctx = context.Background()
	s.PermitChan = make(chan permit.Permit, 1)
	s.Buffer = buffer.New(s.PermitChan, batch.NewSliding(s.MaxSize))
}

func (s *MySuite) TestEmpty(c *C) {
	c.Assert(s.Buffer.Bucket.HighWaterMark, Equals, 4)
	c.Assert(s.Buffer.Bucket.LowWaterMark, Equals, 2)
	c.Assert(s.Buffer.Bucket.WaterLevel, Equals, 0)
}

func (s *MySuite) TestPrefillEmpty(c *C) {
	s.Buffer.Prefill(s.Ctx)
	permit := <-s.PermitChan
	c.Assert(permit.SizeHint, Equals, 4)
	c.Assert(s.Buffer.Bucket.WaterLevel, Equals, 4)
	c.Assert(s.Buffer.BatchingPolicy.Size(), Equals, 0)
}

func (s *MySuite) TestDrainAboveLowWaterMark(c *C) {
	s.Buffer.Prefill(s.Ctx)
	<-s.PermitChan
	s.Buffer.BufferTask(s.Ctx, fixtures.SomeTask{0})
	c.Assert(async.FetchPermit(s.PermitChan), IsNil)
	c.Assert(s.Buffer.Bucket.WaterLevel, Equals, 3)
	c.Assert(s.Buffer.BatchingPolicy.GetBatch(), DeepEquals, batch.Batch{fixtures.SomeTask{0}})
}

func (s *MySuite) TestDrainBelowLowWaterMarkWithBufferPartiallyFilled(c *C) {
	s.Buffer.Prefill(s.Ctx)
	<-s.PermitChan
	s.Buffer.BufferTask(s.Ctx, fixtures.SomeTask{0})
	s.Buffer.BufferTask(s.Ctx, fixtures.SomeTask{1})
	permit := <-s.PermitChan
	c.Assert(s.Buffer.Bucket.WaterLevel, Equals, 4)
	c.Assert(permit.SizeHint, Equals, 2)
}

func (s *MySuite) TestDrainBelowLowWaterMarkWithEmptyBuffer(c *C) {
	s.Buffer.Prefill(s.Ctx)
	<-s.PermitChan
	s.Buffer.BufferTask(s.Ctx, fixtures.SomeTask{0})
	s.Buffer.BufferTask(s.Ctx, fixtures.SomeTask{1})
	batchCh := make(chan batch.Batch, 1)
	s.Buffer.Flush(s.Ctx, batchCh)
	s.Buffer.BufferTask(s.Ctx, fixtures.SomeTask{0})
	permit := <-s.PermitChan
	c.Assert(s.Buffer.Bucket.WaterLevel, Equals, 3)
	c.Assert(permit.SizeHint, Equals, 2)
}

func (s *MySuite) TestFillToTheBrim(c *C) {
	s.Buffer.Prefill(s.Ctx)
	permit := <-s.PermitChan
	c.Assert(permit.SizeHint, Equals, 4)
	s.Buffer.BufferTask(s.Ctx, fixtures.SomeTask{0})
	s.Buffer.BufferTask(s.Ctx, fixtures.SomeTask{1})
	permit = <-s.PermitChan
	c.Assert(permit.SizeHint, Equals, 2)
	s.Buffer.BufferTask(s.Ctx, fixtures.SomeTask{2})
	s.Buffer.BufferTask(s.Ctx, fixtures.SomeTask{3})
	// No permit, buffer full.
	c.Assert(async.FetchPermit(s.PermitChan), IsNil)
}
