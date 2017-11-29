package main

import (
	"github.com/nowthisnews/dp-pubsub-archai/pipeline"
	"time"
)

func main() {
	pipeline.Run(time.Second * 5)
}
