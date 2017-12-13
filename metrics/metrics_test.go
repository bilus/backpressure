package metrics_test

import (
	"errors"
	"github.com/bilus/backpressure/metrics"
	. "github.com/hasSalil/go-check-utils/deepequals"
	. "gopkg.in/check.v1"
	"testing"
	"time"
)

func Test(t *testing.T) { TestingT(t) }

type MySuite struct{}

func (s *MySuite) SetUpTest(c *C) {
	DeltaDeepEquals.UseFloatDelta(0.1)
}

var _ = Suite(&MySuite{})

func (s *MySuite) TestEmpty(c *C) {
	m := metrics.NewBasic("source")
	c.Assert(m.SourceName(), Equals, "source")
	c.Assert(m.Labels(), DeltaDeepEquals, []string{"iterations", "successes", "failures", "avgt"})
	c.Assert(m.Values(), DeltaDeepEquals, []float64{0, 0, 0, 0})
}

func (s *MySuite) TestManualTracking(c *C) {
	m := metrics.NewBasic("source")
	m.Begin(5)
	m.EndWithSuccess(2)
	m.EndWithFailure(3)
	c.Assert(m.Values(), DeltaDeepEquals, []float64{5, 2, 3, 0})
}

func (s *MySuite) TestAutoTrackingSuccess(c *C) {
	m := metrics.NewBasic("source")
	span := m.Begin(5)
	func() (err error) {
		defer span.Close(&err)
		return nil
	}()
	c.Assert(m.Values(), DeltaDeepEquals, []float64{5, 5, 0, 0})
}

func (s *MySuite) TestAutoTrackingFailure(c *C) {
	m := metrics.NewBasic("source")
	span := m.Begin(5)
	func() (err error) {
		defer span.Close(&err)
		return errors.New("Ooops!")
	}()
	c.Assert(m.Values(), DeltaDeepEquals, []float64{5, 0, 5, 0})
}

func (s *MySuite) TestAutoTrackingWithContinue(c *C) {
	m := metrics.NewBasic("source")
	span := m.Begin(5)
	span.Continue(5)
	func() (err error) {
		defer span.Close(&err)
		return errors.New("Ooops!")
	}()
	c.Assert(m.Values(), DeltaDeepEquals, []float64{10, 0, 10, 0})
}

func (s *MySuite) TestMovingAverage(c *C) {
	m := metrics.NewBasic("source")
	span1 := m.Begin(50)
	time.Sleep(time.Millisecond * 500)
	var err error
	span1.Close(&err)

	c.Assert(m.Values(), DeltaDeepEquals, []float64{50, 50, 0, 0.50})

	span2 := m.Begin(1)
	time.Sleep(time.Millisecond * 100)
	err = errors.New("Oops!")
	span2.Close(&err)
	c.Assert(m.Values(), DeltaDeepEquals, []float64{51, 50, 1, 0.48})
}
