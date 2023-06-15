package main

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type nodeWrapper struct {
	node *maelstrom.Node

	counterMu sync.RWMutex
	counter   map[string]int

	initDone chan struct{}
}

func newNodeWrapper(node *maelstrom.Node) *nodeWrapper {
	return &nodeWrapper{
		node:     node,
		counter:  make(map[string]int),
		initDone: make(chan struct{}),
	}
}

func (n *nodeWrapper) initHandler(msg maelstrom.Message) error {
	n.counterMu.Lock()
	defer n.counterMu.Unlock()

	// NodeIDs() is guaranteed to be populated by now since
	// the handler is invoked on the init message only after
	// the message is processed and node IDs extracted.
	for _, node := range n.node.NodeIDs() {
		n.counter[node] = 0
	}

	n.initDone <- struct{}{}

	return nil
}

func (n *nodeWrapper) addCounterHandler(msg maelstrom.Message) error {
	n.node.Reply(msg, map[string]any{
		"type": "add_ok",
	})

	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	delta := int(body["delta"].(float64))
	n.counterMu.Lock()
	n.counter[n.node.ID()] += delta
	n.counterMu.Unlock()

	return nil
}

func (n *nodeWrapper) readCounterHandler(msg maelstrom.Message) error {
	res := 0
	n.counterMu.RLock()
	for _, count := range n.counter {
		res += count
	}
	n.counterMu.RUnlock()

	return n.node.Reply(msg, map[string]any{
		"type":  "read_ok",
		"value": res,
	})
}

func (n *nodeWrapper) mergeHandler(msg maelstrom.Message) error {
	var body mergeMessage
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	peerCounter := body.Counter
	n.counterMu.Lock()
	defer n.counterMu.Unlock()

	// No need to worry about keys not existing etc, on receipt
	// of init message, each node's counter is initialized with
	// all nodes' IDs as keys.
	for k := range peerCounter {
		n.counter[k] = max(n.counter[k], peerCounter[k])
	}

	return nil
}

func (n *nodeWrapper) startAsyncReplication(ctx context.Context) {
	// Wait for init message to come otherwise NodeIDs()
	// will be empty and no replication will take place.
	<-n.initDone
	nodes := n.node.NodeIDs()
	t := time.NewTicker(3 * time.Second)
	defer t.Stop()

	done := false
	for {
		select {
		case <-t.C:
			for _, node := range nodes {
				// Don't replicate to self.
				if node == n.node.ID() {
					continue
				}
				n.counterMu.RLock()
				n.node.Send(node, map[string]any{
					"type":    "merge",
					"counter": n.counter,
				})
				n.counterMu.RUnlock()
			}
		case <-ctx.Done():
			done = true
		}
		if done {
			break
		}
	}
}

type mergeMessage struct {
	Counter map[string]int `json:"counter"`
}

func max(a, b int) int {
	if a < b {
		return b
	}

	return a
}
