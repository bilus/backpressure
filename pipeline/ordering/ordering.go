package ordering

import (
	"github.com/bilus/backpressure/pipeline/batch"
)

type OrderingPolicy interface {
	Sort(batch.Batch) error
}

var None = Unordered{}

type Unordered struct{}

func (u Unordered) Sort(batch.Batch) error {
	return nil
}
