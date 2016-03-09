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
	"encoding/gob"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"syscall"

	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"

	"golang.org/x/net/context"
)

const Debug = 1
const ServTag string = "serv"

func socketPort(tag string, host int) string {
	s := "/var/tmp/rs-"
	s += strconv.Itoa(os.Getuid()) + "/"
	os.Mkdir(s, 0777)
	// s += "rd-"
	s += strconv.Itoa(os.Getpid()) + "-"
	s += tag + "-"
	s += strconv.Itoa(host)
	return s
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

//Op Basic Raft OP here
type Op struct {
}

type KVRaft struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	raftNode   *node

	// Your definitions here.
}

func (kv *KVRaft) callback(to uint64, ctx context.Context, message raftpb.Message) {
	// args := MsgArgs{Ctx: "ctx", Message: message}
	var reply MsgReply
	var serAddr string
	if to == 1 {
		serAddr = ":1234"
	} else {
		serAddr = ":1235"
	}

	log.Println("[CALLBACK]: IM:", kv.me, "->", to, " addr:", serAddr, " ctx:", ctx)

	call(serAddr, "KVRaft.Msg", &message, &reply)

	log.Println("[CALLBACK] RPC done", kv.me, reply)
	return
}

//Msg Deliver msg for raft status exchange
func (kv *KVRaft) Msg(args *raftpb.Message, reply *MsgReply) error {
	log.Println("[MSG]", *args)
	kv.raftNode.raft.Step(kv.raftNode.ctx, *args)
	return nil
}

func (kv *KVRaft) Get(args *GetArgs, reply *GetReply) error {
	log.Println("[GET]", args)
	if args.Key == "" {
		log.Println("[GET]", InvalidParam)
		return errors.New(InvalidParam)
	}

	for k, v := range kv.raftNode.pstore {
		log.Println("[GET]  data", k, v)
	}
	if v, exist := kv.raftNode.pstore[args.Key]; exist {
		reply.Value = v
		return nil
	}

	reply.Err = ErrNoKey
	return errors.New(ErrNoKey)
}

func (kv *KVRaft) Put(args *PutArgs, reply *PutReply) error {
	log.Println("[PUT]", args)

	if args.Key == "" || args.Value == "" {
		log.Println("[PUT]", InvalidParam)
		err := errors.New(InvalidParam)
		reply.Err = InvalidParam
		return err
	}

	if _, exist := kv.raftNode.pstore[args.Key]; exist {
		reply.PreviousValue = kv.raftNode.pstore[args.Key]
	}

	reply.Err = "NIL"

	data := fmt.Sprintf("%s:%s", args.Key, args.Value)
	byteData := []byte(data)
	log.Println("[PUT] ", byteData)
	kv.raftNode.raft.Propose(kv.raftNode.ctx, byteData)
	return nil
}

// tell the server to shut itself down.
// please do not change this function.
func (kv *KVRaft) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	kv.dead = true
	kv.l.Close()
	kv.raftNode.stop()
	//remove socket file
	os.Remove(socketPort(ServTag, kv.me))
}

func StartServer(serversPort string, me int) *KVRaft {
	return startServer(serversPort, me, []raft.Peer{{ID: uint64(me)}})
}

func StartClusterServers(serversPort string, me int, cluster []raft.Peer) *KVRaft {
	return startServer(serversPort, me, cluster)
}

func StarServerJoinCluster() {
	//TODO
	// nodes[3] = newNode(3, []raft.Peer{})
	// go nodes[3].run()
	// nodes[2].raft.ProposeConfChange(nodes[2].ctx, raftpb.ConfChange{
	// 	ID:      3,
	// 	Type:    raftpb.ConfChangeAddNode,
	// 	NodeID:  3,
	// 	Context: []byte(""),
	// })
}

func startServer(serversPort string, me int, cluster []raft.Peer) *KVRaft {
	gob.Register(Op{})
	// gob.Register(context.Background())
	kv := new(KVRaft)
	kv.me = me

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.raftNode = newNode(uint64(me), cluster, kv)
	go kv.raftNode.run()

	// Wait for leader, is there a better way to do this
	// log.Println("Wait for leader")
	// for kv.raftNode.raft.Status().Lead != 1 {
	// 	time.Sleep(100 * time.Millisecond)
	// }

	// log.Println("Wait for hearbit")
	// time.Sleep(2000 * time.Millisecond)

	// socketFile := socketPort(ServTag, me)
	// if _, err := os.Stat(socketFile); err == nil {
	// 	//socket exist
	os.Remove(serversPort)
	// }

	log.Println("[server] ", me, " ==> ", serversPort)
	l, e := net.Listen("tcp", serversPort)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	go func() {
		for kv.dead == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.dead == false {
				if kv.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.unreliable && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.dead == false {
				fmt.Printf("KVRaft(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
