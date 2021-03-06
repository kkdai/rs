// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rs

import (
	"bytes"
	"fmt"
	"log"
	"math"
	"time"

	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"golang.org/x/net/context"
)

const hb = 1

type node struct {
	id          uint64
	ctx         context.Context
	pstore      map[string]string
	store       *raft.MemoryStorage
	cfg         *raft.Config
	raft        raft.Node
	ticker      <-chan time.Time
	done        <-chan struct{}
	msgCallback *KVRaft
}

func newNode(id uint64, peers []raft.Peer, fn *KVRaft) *node {
	store := raft.NewMemoryStorage()
	n := &node{
		id:    id,
		ctx:   context.TODO(),
		store: store,
		cfg: &raft.Config{
			ID:              id,
			ElectionTick:    10 * hb,
			HeartbeatTick:   hb,
			Storage:         store,
			MaxSizePerMsg:   math.MaxUint16,
			MaxInflightMsgs: 256,
		},
		pstore: make(map[string]string),
		ticker: time.Tick(time.Second),
		done:   make(chan struct{}),
	}

	n.raft = raft.StartNode(n.cfg, peers)
	n.msgCallback = fn
	return n
}

func (n *node) run() {

	for {
		select {
		case <-n.ticker:
			n.raft.Tick()
		case rd := <-n.raft.Ready():
			n.saveToStorage(rd.HardState, rd.Entries, rd.Snapshot)
			n.send(rd.Messages)
			if !raft.IsEmptySnap(rd.Snapshot) {
				n.processSnapshot(rd.Snapshot)
			}
			for _, entry := range rd.CommittedEntries {
				n.process(entry)
				if entry.Type == raftpb.EntryConfChange {
					var cc raftpb.ConfChange
					cc.Unmarshal(entry.Data)
					n.raft.ApplyConfChange(cc)
				}
			}
			n.raft.Advance()
		case <-n.done:
			return
		}
	}
}

func (n *node) saveToStorage(hardState raftpb.HardState, entries []raftpb.Entry, snapshot raftpb.Snapshot) {
	n.store.Append(entries)

	if !raft.IsEmptyHardState(hardState) {
		n.store.SetHardState(hardState)
	}

	if !raft.IsEmptySnap(snapshot) {
		n.store.ApplySnapshot(snapshot)
	}
}

func (n *node) send(messages []raftpb.Message) {
	for _, m := range messages {
		log.Println(raft.DescribeMessage(m, nil))

		// send message to other node
		//nodes[int(m.To)].receive(n.ctx, m)
		// log.Println("[node]Send From:", n.id, "->", m.To, ":", m)
		n.msgCallback.callback(m.To, n.ctx, m)
	}
}

func (n *node) processSnapshot(snapshot raftpb.Snapshot) {
	panic(fmt.Sprintf("Applying snapshot on node %v is not implemented", n.id))
}

func (n *node) process(entry raftpb.Entry) {
	log.Printf("node %v: processing entry: %v\n", n.id, entry)
	if entry.Type == raftpb.EntryNormal && entry.Data != nil {
		parts := bytes.SplitN(entry.Data, []byte(":"), 2)
		n.pstore[string(parts[0])] = string(parts[1])
	}
}

//func (n *node) receive(ctx context.Context, message raftpb.Message) {
//n.raft.Step(ctx, message)
//}

func (n *node) stop() {
	//n.done <- struct{}
}

// var (
// 	nodes = make(map[int]*node)
// )

// func main() {
// 	// start a small cluster
// 	nodes[1] = newNode(1, []raft.Peer{{ID: 1}, {ID: 2}, {ID: 3}})
// 	nodes[1].raft.Campaign(nodes[1].ctx)
// 	go nodes[1].run()

// 	nodes[2] = newNode(2, []raft.Peer{{ID: 1}, {ID: 2}, {ID: 3}})
// 	go nodes[2].run()

// 	nodes[3] = newNode(3, []raft.Peer{})
// 	go nodes[3].run()
// 	nodes[2].raft.ProposeConfChange(nodes[2].ctx, raftpb.ConfChange{
// 		ID:      3,
// 		Type:    raftpb.ConfChangeAddNode,
// 		NodeID:  3,
// 		Context: []byte(""),
// 	})

// 	// Wait for leader, is there a better way to do this
// 	for nodes[1].raft.Status().Lead != 1 {
// 		time.Sleep(100 * time.Millisecond)
// 	}

// 	nodes[1].raft.Propose(nodes[1].ctx, []byte("mykey1:myvalue1"))
// 	nodes[2].raft.Propose(nodes[2].ctx, []byte("mykey2:myvalue2"))
// 	nodes[3].raft.Propose(nodes[3].ctx, []byte("mykey3:myvalue3"))

// 	// Wait for proposed entry to be commited in cluster.
// 	// Apperently when should add an uniq id to the message and wait until it is
// 	// commited in the node.
// 	fmt.Printf("** Sleeping to visualize heartbeat between nodes **\n")
// 	time.Sleep(2000 * time.Millisecond)

// 	// Just check that data has been persited
// 	for i, node := range nodes {
// 		fmt.Printf("** Node %v **\n", i)
// 		for k, v := range node.pstore {
// 			fmt.Printf("%v = %v\n", k, v)
// 		}
// 		fmt.Printf("*************\n")
// 	}
// }
