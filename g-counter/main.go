package main

import (
	"context"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	nWrapper := newNodeWrapper(n)

	n.Handle("init", nWrapper.initHandler)
	n.Handle("add", nWrapper.addCounterHandler)
	n.Handle("read", nWrapper.readCounterHandler)
	n.Handle("merge", nWrapper.mergeHandler)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go nWrapper.startAsyncReplication(ctx)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
