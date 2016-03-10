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
	"hash/fnv"

	"golang.org/x/net/context"

	"github.com/coreos/etcd/raft/raftpb"
)

const (
	OK           = "OK"
	ErrNoKey     = "ErrNoKey"
	InvalidParam = "Invalid Parameter"
)

type Err string

type PutArgs struct {
	Key    string
	Value  string
	DoHash bool // For PutHash
}

type PutReply struct {
	Err           Err
	PreviousValue string // For PutHash
}

type GetArgs struct {
	Key string
}

type GetReply struct {
	Err   Err
	Value string
}

type MsgArgs struct {
	Ctx     context.Context
	Message raftpb.Message
}

type MsgReply struct {
	Err Err
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
