package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type set struct {
	h map[string]struct{}
}

// absorb is like union but does a set union in-place.
func (s *set) absorb(incoming set) {
	for k := range incoming.h {
		s.h[k] = struct{}{}
	}
}

func (s *set) insert(ele string) {
	s.h[ele] = struct{}{}
}

type nodeWrapper struct {
	node *maelstrom.Node

	setMu sync.RWMutex
	set   *set

	initDone chan struct{}
}

func newNodeWrapper(node *maelstrom.Node) *nodeWrapper {
	return &nodeWrapper{
		node:     node,
		set:      &set{h: make(map[string]struct{})},
		initDone: make(chan struct{}),
	}
}

func (n *nodeWrapper) initHandler(msg maelstrom.Message) error {
	n.initDone <- struct{}{}
	return nil
}

func (n *nodeWrapper) addSetHandler(msg maelstrom.Message) error {
	n.node.Reply(msg, map[string]any{
		"type": "add_ok",
	})

	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	element := body["element"]
	n.setMu.Lock()
	defer n.setMu.Unlock()

	n.set.insert(fmt.Sprint(element))
	return nil
}

func (n *nodeWrapper) readSetHandler(msg maelstrom.Message) error {
	res := make([]any, 0, len(n.set.h))
	n.setMu.RLock()
	for ele := range n.set.h {
		res = append(res, toInt(ele))
	}
	n.setMu.RUnlock()

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

	n.setMu.Lock()
	defer n.setMu.Unlock()

	log.Print("Before:", len(n.set.h))
	n.set.absorb(set{body.Set})
	log.Print("After:", len(n.set.h))
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
				n.setMu.RLock()
				n.node.Send(node, map[string]any{
					"type": "merge",
					"set":  n.set.h,
				})
				n.setMu.RUnlock()
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
	Set map[string]struct{} `json:"set"`
}

func toInt(s string) int {
	res, _ := strconv.Atoi(s)
	return res
}
