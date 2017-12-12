package bucket_test

import (
	"context"
	"github.com/bilus/backpressure/pipeline/bucket"
	"github.com/bilus/backpressure/pipeline/permit"
	. "gopkg.in/check.v1"
	"testing"
	"time"
)

var _ = time.Now

func Test(t *testing.T) { TestingT(t) }

type MySuite struct {
	PermitChan chan permit.Permit
	Bucket     bucket.Bucket
	Ctx        context.Context
	LWM        int
	HWM        int
}

var _ = Suite(&MySuite{})

func (s *MySuite) SetUpTest(c *C) {
	s.LWM = 2
	s.HWM = 4
	s.Ctx = context.Background()
	s.PermitChan = make(chan permit.Permit, 1)
	s.Bucket = bucket.New(s.PermitChan, s.LWM, s.HWM)
}

func (s *MySuite) TestEmpty(c *C) {
	c.Assert(s.Bucket.HighWaterMark, Equals, s.HWM)
	c.Assert(s.Bucket.LowWaterMark, Equals, s.LWM)
	c.Assert(s.Bucket.WaterLevel, Equals, 0)
	c.Assert(s.Bucket.RefillNeeded(), Equals, 4)
}

func (s *MySuite) TestFillUpEmpty(c *C) {
	s.Bucket.FillUp(s.Ctx, s.Bucket.RefillNeeded())
	permit := <-s.PermitChan
	c.Assert(permit.SizeHint, Equals, s.HWM)
	c.Assert(s.Bucket.WaterLevel, Equals, s.HWM)
}

func (s *MySuite) TestDrainAboveLowWaterMark(c *C) {
	s.Bucket.FillUp(s.Ctx, s.Bucket.RefillNeeded())
	<-s.PermitChan
	s.Bucket.Drain(s.Ctx, 1)
	c.Assert(s.Bucket.WaterLevel, Equals, s.HWM-1)
	c.Assert(s.Bucket.RefillNeeded(), Equals, 0)
}

func (s *MySuite) TestDrainBelowLowWaterMark(c *C) {
	s.Bucket.FillUp(s.Ctx, s.Bucket.RefillNeeded())
	<-s.PermitChan
	s.Bucket.Drain(s.Ctx, 3)
	c.Assert(s.Bucket.RefillNeeded(), Equals, 3)
	s.Bucket.FillUp(s.Ctx, s.Bucket.RefillNeeded())
	permit := <-s.PermitChan
	c.Assert(permit.SizeHint, Equals, 3)
	c.Assert(s.Bucket.WaterLevel, Equals, s.HWM)
}

func (s *MySuite) TestSendingPermitCanBeInterrupted(c *C) {
	var cancel context.CancelFunc
	s.Ctx, cancel = context.WithTimeout(s.Ctx, time.Microsecond*100)
	defer cancel()
	s.PermitChan = make(chan permit.Permit)
	s.Bucket = bucket.New(s.PermitChan, s.LWM, s.HWM)
	s.Bucket.FillUp(s.Ctx, s.Bucket.RefillNeeded())
}

func (s *MySuite) TestFillUpBelowCapacity(c *C) {
	s.Bucket.FillUp(s.Ctx, s.Bucket.RefillNeeded())
	<-s.PermitChan
	s.Bucket.Drain(s.Ctx, 3)
	c.Assert(s.Bucket.RefillNeeded(), Equals, 3)
	s.Bucket.FillUp(s.Ctx, 1)
	permit := <-s.PermitChan
	c.Assert(permit.SizeHint, Equals, 1)
	c.Assert(s.Bucket.WaterLevel, Equals, s.HWM-2)
	c.Assert(s.Bucket.RefillNeeded(), Equals, 2)
}
