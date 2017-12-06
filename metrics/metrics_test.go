package metrics_test

import (
	"errors"
	"github.com/bilus/backpressure/metrics"
	. "gopkg.in/check.v1"
	"testing"
)

func Test(t *testing.T) { TestingT(t) }

type MySuite struct{}

var _ = Suite(&MySuite{})

func (s *MySuite) TestEmpty(c *C) {
	m := metrics.NewBasic("source")
	c.Assert(m.SourceName(), Equals, "source")
	c.Assert(m.Labels(), DeepEquals, []string{"iterations", "successes", "failures"})
	c.Assert(m.Values(), DeepEquals, []string{"0", "0", "0"})
}

func (s *MySuite) TestManualTracking(c *C) {
	m := metrics.NewBasic("source")
	m.Begin(5)
	m.EndWithSuccess(2)
	m.EndWithFailure(3)
	c.Assert(m.Values(), DeepEquals, []string{"5", "2", "3"})
}

func (s *MySuite) TestAutoTrackingSuccess(c *C) {
	m := metrics.NewBasic("source")
	span := m.Begin(5)
	func() (err error) {
		defer span.Close(&err)
		return nil
	}()
	c.Assert(m.Values(), DeepEquals, []string{"5", "5", "0"})
}

func (s *MySuite) TestAutoTrackingFailure(c *C) {
	m := metrics.NewBasic("source")
	span := m.Begin(5)
	func() (err error) {
		defer span.Close(&err)
		return errors.New("Ooops!")
	}()
	c.Assert(m.Values(), DeepEquals, []string{"5", "0", "5"})
}

func (s *MySuite) TestAutoTrackingWithContinue(c *C) {
	m := metrics.NewBasic("source")
	span := m.Begin(5)
	span.Continue(5)
	func() (err error) {
		defer span.Close(&err)
		return errors.New("Ooops!")
	}()
	c.Assert(m.Values(), DeepEquals, []string{"10", "0", "10"})
}
