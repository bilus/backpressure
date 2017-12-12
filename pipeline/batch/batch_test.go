package batch_test

import (
	"github.com/bilus/backpressure/pipeline/batch"
	"github.com/bilus/backpressure/test/fixtures"
	. "gopkg.in/check.v1"
	"testing"
)

func Test(t *testing.T) { TestingT(t) }

type MySuite struct{}

var _ = Suite(&MySuite{})

func (s *MySuite) TestSlidingBuffering(c *C) {
	sliding := batch.NewSliding(4)
	sliding.AddTask(fixtures.SomeTask{0})
	sliding.AddTask(fixtures.SomeTask{1})
	sliding.AddTask(fixtures.SomeTask{2})
	c.Assert(sliding.GetBatch(), DeepEquals, batch.Batch{
		fixtures.SomeTask{0},
		fixtures.SomeTask{1},
		fixtures.SomeTask{2},
	})
	c.Assert(sliding.Size(), Equals, 3)
	sliding.AddTask(fixtures.SomeTask{3})
	c.Assert(sliding.GetBatch(), DeepEquals, batch.Batch{
		fixtures.SomeTask{0},
		fixtures.SomeTask{1},
		fixtures.SomeTask{2},
		fixtures.SomeTask{3},
	})
	c.Assert(sliding.Size(), Equals, 4)
	c.Assert(sliding.AddTask(fixtures.SomeTask{4}), Equals, batch.Dropped{fixtures.SomeTask{0}})
	c.Assert(sliding.GetBatch(), DeepEquals, batch.Batch{
		fixtures.SomeTask{4},
		fixtures.SomeTask{1},
		fixtures.SomeTask{2},
		fixtures.SomeTask{3},
	})
	c.Assert(sliding.Size(), Equals, 4)
	c.Assert(sliding.AddTask(fixtures.SomeTask{5}), Equals, batch.Dropped{fixtures.SomeTask{1}})
	c.Assert(sliding.GetBatch(), DeepEquals, batch.Batch{
		fixtures.SomeTask{4},
		fixtures.SomeTask{5},
		fixtures.SomeTask{2},
		fixtures.SomeTask{3},
	})
	c.Assert(sliding.Size(), Equals, 4)
	c.Assert(sliding.AddTask(fixtures.SomeTask{6}), Equals, batch.Dropped{fixtures.SomeTask{2}})
	c.Assert(sliding.GetBatch(), DeepEquals, batch.Batch{
		fixtures.SomeTask{4},
		fixtures.SomeTask{5},
		fixtures.SomeTask{6},
		fixtures.SomeTask{3},
	})
	c.Assert(sliding.Size(), Equals, 4)
	c.Assert(sliding.AddTask(fixtures.SomeTask{7}), Equals, batch.Dropped{fixtures.SomeTask{3}})
	c.Assert(sliding.GetBatch(), DeepEquals, batch.Batch{
		fixtures.SomeTask{4},
		fixtures.SomeTask{5},
		fixtures.SomeTask{6},
		fixtures.SomeTask{7},
	})
	c.Assert(sliding.Size(), Equals, 4)
	c.Assert(sliding.AddTask(fixtures.SomeTask{8}), Equals, batch.Dropped{fixtures.SomeTask{4}})
	c.Assert(sliding.GetBatch(), DeepEquals, batch.Batch{
		fixtures.SomeTask{8},
		fixtures.SomeTask{5},
		fixtures.SomeTask{6},
		fixtures.SomeTask{7},
	})
}

func (s *MySuite) TestDroppingBuffering(c *C) {
	dropping := batch.NewDropping(4)
	dropping.AddTask(fixtures.SomeTask{0})
	dropping.AddTask(fixtures.SomeTask{1})
	dropping.AddTask(fixtures.SomeTask{2})
	c.Assert(dropping.GetBatch(), DeepEquals, batch.Batch{
		fixtures.SomeTask{0},
		fixtures.SomeTask{1},
		fixtures.SomeTask{2},
	})
	c.Assert(dropping.Size(), Equals, 3)
	dropping.AddTask(fixtures.SomeTask{3})
	c.Assert(dropping.GetBatch(), DeepEquals, batch.Batch{
		fixtures.SomeTask{0},
		fixtures.SomeTask{1},
		fixtures.SomeTask{2},
		fixtures.SomeTask{3},
	})
	c.Assert(dropping.Size(), Equals, 4)
	c.Assert(dropping.AddTask(fixtures.SomeTask{4}), Equals, batch.Dropped{fixtures.SomeTask{4}})
	c.Assert(dropping.GetBatch(), DeepEquals, batch.Batch{
		fixtures.SomeTask{0},
		fixtures.SomeTask{1},
		fixtures.SomeTask{2},
		fixtures.SomeTask{3},
	})
	c.Assert(dropping.Size(), Equals, 4)
}
