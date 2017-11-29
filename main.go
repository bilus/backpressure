package main

import (
	"./pipeline"
	"time"
)

func main() {
	pipeline.Run(time.Second * 5)
}
