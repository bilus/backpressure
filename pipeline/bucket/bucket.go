package bucket

import (
	"context"
	"errors"
	"fmt"
	"github.com/bilus/backpressure/colors"
	"github.com/bilus/backpressure/pipeline/permit"
	"log"
)

type Bucket struct {
	PermitCh      chan<- permit.Permit
	LowWaterMark  int
	HighWaterMark int
	WaterLevel    int
}

var NegWaterLevel = errors.New("Water level below zero")

func New(permitCh chan<- permit.Permit, lowWaterMark int, highWaterMark int) Bucket {
	if highWaterMark <= lowWaterMark {
		panic(fmt.Sprintf("HighWaterMark must be higher than lowWaterMark h=%v l=%v", highWaterMark, lowWaterMark))
	}
	return Bucket{
		PermitCh:      permitCh,
		LowWaterMark:  lowWaterMark,
		HighWaterMark: highWaterMark,
	}
}

func (b *Bucket) FillUp(ctx context.Context, maxAmount int) error {
	delta := b.RefillNeeded()
	if delta > maxAmount {
		delta = maxAmount
	}
	if delta <= 0 {
		return nil
	}
	newPermit := permit.New(delta)
	log.Printf(colors.Magenta("Sending permit: %v (hwm=%v wl=%v lwm=%v)"), newPermit, b.HighWaterMark, b.WaterLevel, b.LowWaterMark)
	select {
	case b.PermitCh <- newPermit:
		b.WaterLevel += delta
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *Bucket) Drain(ctx context.Context, amount int) error {
	if b.WaterLevel <= 0 {
		return NegWaterLevel
	}
	b.WaterLevel -= amount
	return nil
}

func (b *Bucket) RefillNeeded() int {
	if b.WaterLevel <= b.LowWaterMark {
		return b.HighWaterMark - b.WaterLevel
	} else {
		return 0
	}
}
