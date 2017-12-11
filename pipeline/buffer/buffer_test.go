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
	HWM        int
	LWM        int
	PermitChan chan permit.Permit
	Buffer     *buffer.Buffer
	Ctx        context.Context
}

var _ = Suite(&MySuite{})

func (s *MySuite) SetUpTest(c *C) {
	s.HWM = 4
	s.LWM = 2
	s.Ctx = context.Background()
	s.PermitChan = make(chan permit.Permit, 1)
	s.Buffer = buffer.New(s.PermitChan, batch.NewSliding(s.HWM), s.LWM)
}

func (s *MySuite) TestEmpty(c *C) {
	c.Assert(s.Buffer.Bucket.HighWaterMark, Equals, s.HWM)
	c.Assert(s.Buffer.Bucket.LowWaterMark, Equals, s.LWM)
	c.Assert(s.Buffer.Bucket.WaterLevel, Equals, 0)
}

func (s *MySuite) TestPrefillEmpty(c *C) {
	s.Buffer.Prefill(s.Ctx)
	permit := <-s.PermitChan
	c.Assert(permit.SizeHint, Equals, s.HWM)
	c.Assert(s.Buffer.Bucket.WaterLevel, Equals, s.HWM)
	c.Assert(s.Buffer.BatchingPolicy.Size(), Equals, 0)
}

func (s *MySuite) TestDrainAboveLowWaterMark(c *C) {
	s.Buffer.Prefill(s.Ctx)
	<-s.PermitChan
	s.Buffer.BufferTask(s.Ctx, fixtures.SomeTask{0})
	c.Assert(async.FetchPermit(s.PermitChan), IsNil)
	c.Assert(s.Buffer.Bucket.WaterLevel, Equals, s.HWM-1)
	c.Assert(s.Buffer.BatchingPolicy.GetBatch(), DeepEquals, batch.Batch{fixtures.SomeTask{0}})
}

func (s *MySuite) TestDrainBelowLowWaterMark(c *C) {
	s.Buffer.Prefill(s.Ctx)
	<-s.PermitChan
	s.Buffer.BufferTask(s.Ctx, fixtures.SomeTask{0})
	s.Buffer.BufferTask(s.Ctx, fixtures.SomeTask{1})
	permit := <-s.PermitChan
	c.Assert(permit.SizeHint, Equals, 2)
	c.Assert(s.Buffer.Bucket.WaterLevel, Equals, s.HWM)
	c.Assert(s.Buffer.BatchingPolicy.GetBatch(), DeepEquals, batch.Batch{
		fixtures.SomeTask{0},
		fixtures.SomeTask{1},
	})
}
