package batch_test

import (
	"github.com/bilus/backpressure/pipeline/batch"
	. "gopkg.in/check.v1"
	"testing"
)

func Test(t *testing.T) { TestingT(t) }

type MySuite struct{}

var _ = Suite(&MySuite{})

type SomeTask struct {
	Id int
}

func (_ SomeTask) TaskTypeTag() {}

func (s *MySuite) TestSlidingBuffering(c *C) {
	sliding := batch.NewSliding(4)
	sliding.AddTask(SomeTask{0})
	sliding.AddTask(SomeTask{1})
	sliding.AddTask(SomeTask{2})
	c.Assert(sliding.GetBatch(), DeepEquals, batch.Batch{
		SomeTask{0},
		SomeTask{1},
		SomeTask{2},
	})
	sliding.AddTask(SomeTask{3})
	c.Assert(sliding.GetBatch(), DeepEquals, batch.Batch{
		SomeTask{0},
		SomeTask{1},
		SomeTask{2},
		SomeTask{3},
	})
	c.Assert(sliding.AddTask(SomeTask{4}), Equals, batch.Dropped{SomeTask{0}})
	c.Assert(sliding.GetBatch(), DeepEquals, batch.Batch{
		SomeTask{4},
		SomeTask{1},
		SomeTask{2},
		SomeTask{3},
	})
	c.Assert(sliding.AddTask(SomeTask{5}), Equals, batch.Dropped{SomeTask{1}})
	c.Assert(sliding.GetBatch(), DeepEquals, batch.Batch{
		SomeTask{4},
		SomeTask{5},
		SomeTask{2},
		SomeTask{3},
	})
	c.Assert(sliding.AddTask(SomeTask{6}), Equals, batch.Dropped{SomeTask{2}})
	c.Assert(sliding.GetBatch(), DeepEquals, batch.Batch{
		SomeTask{4},
		SomeTask{5},
		SomeTask{6},
		SomeTask{3},
	})
	c.Assert(sliding.AddTask(SomeTask{7}), Equals, batch.Dropped{SomeTask{3}})
	c.Assert(sliding.GetBatch(), DeepEquals, batch.Batch{
		SomeTask{4},
		SomeTask{5},
		SomeTask{6},
		SomeTask{7},
	})
	c.Assert(sliding.AddTask(SomeTask{8}), Equals, batch.Dropped{SomeTask{4}})
	c.Assert(sliding.GetBatch(), DeepEquals, batch.Batch{
		SomeTask{8},
		SomeTask{5},
		SomeTask{6},
		SomeTask{7},
	})
}

func (s *MySuite) TestDroppingBuffering(c *C) {
	dropping := batch.NewDropping(4)
	dropping.AddTask(SomeTask{0})
	dropping.AddTask(SomeTask{1})
	dropping.AddTask(SomeTask{2})
	c.Assert(dropping.GetBatch(), DeepEquals, batch.Batch{
		SomeTask{0},
		SomeTask{1},
		SomeTask{2},
	})
	dropping.AddTask(SomeTask{3})
	c.Assert(dropping.GetBatch(), DeepEquals, batch.Batch{
		SomeTask{0},
		SomeTask{1},
		SomeTask{2},
		SomeTask{3},
	})
	c.Assert(dropping.AddTask(SomeTask{4}), Equals, batch.Dropped{SomeTask{4}})
	c.Assert(dropping.GetBatch(), DeepEquals, batch.Batch{
		SomeTask{0},
		SomeTask{1},
		SomeTask{2},
		SomeTask{3},
	})
}
