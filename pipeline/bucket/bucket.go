package bucket

import (
	"context"
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

func (b *Bucket) FillUp(ctx context.Context) error {
	delta := b.HighWaterMark - b.WaterLevel
	if delta <= 0 {
		return nil
	}
	newPermit := permit.New(delta)
	log.Printf(colors.Magenta("Sending permit: %v"), newPermit)
	select {
	case b.PermitCh <- newPermit:
		b.WaterLevel = b.HighWaterMark
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *Bucket) Drain(ctx context.Context, amount int) error {
	b.WaterLevel -= amount
	if b.WaterLevel <= b.LowWaterMark {
		return b.FillUp(ctx)
	} else {
		return nil
	}
}
