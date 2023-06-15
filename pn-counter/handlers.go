package main

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type counter struct {
	sync.RWMutex
	count map[string]int
}

func (c *counter) initialize(nodes []string) {
	c.Lock()
	defer c.Unlock()

	for _, node := range nodes {
		c.count[node] = 0
	}
}

func (c *counter) add(node string, value int) {
	c.Lock()
	defer c.Unlock()

	c.count[node] += value
}

func (c *counter) read() int {
	c.RLock()
	defer c.RUnlock()

	res := 0
	for _, nodeCount := range c.count {
		res += nodeCount
	}

	return res
}

func (c *counter) merge(peerCounter map[string]int) {
	c.Lock()
	defer c.Unlock()

	for k := range peerCounter {
		c.count[k] = max(c.count[k], peerCounter[k])
	}
}

type nodeWrapper struct {
	node *maelstrom.Node

	pCounter *counter
	nCounter *counter

	initDone chan struct{}
}

func newNodeWrapper(node *maelstrom.Node) *nodeWrapper {
	return &nodeWrapper{
		node:     node,
		pCounter: &counter{count: make(map[string]int)},
		nCounter: &counter{count: make(map[string]int)},
		initDone: make(chan struct{}),
	}
}

func (n *nodeWrapper) initHandler(msg maelstrom.Message) error {
	// NodeIDs() is guaranteed to be populated by now since
	// the handler is invoked on the init message only after
	// the message is processed and node IDs extracted.
	nodes := n.node.NodeIDs()

	n.pCounter.initialize(nodes)
	n.nCounter.initialize(nodes)

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
	if delta < 0 {
		n.nCounter.add(n.node.ID(), delta*-1)
		return nil
	}

	n.pCounter.add(n.node.ID(), delta)
	return nil
}

func (n *nodeWrapper) readCounterHandler(msg maelstrom.Message) error {
	return n.node.Reply(msg, map[string]any{
		"type":  "read_ok",
		"value": n.pCounter.read() - n.nCounter.read(),
	})
}

func (n *nodeWrapper) mergeHandler(msg maelstrom.Message) error {
	var body mergeMessage
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	n.pCounter.merge(body.Pcounter)
	n.nCounter.merge(body.Ncounter)
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
				n.node.Send(node, map[string]any{
					"type":     "merge",
					"pcounter": n.pCounter.count,
					"ncounter": n.nCounter.count,
				})
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
	Pcounter map[string]int `json:"pcounter"`
	Ncounter map[string]int `json:"ncounter"`
}

func max(a, b int) int {
	if a < b {
		return b
	}

	return a
}
