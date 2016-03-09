package rs

import (
	"log"
	"testing"
	"time"

	"github.com/coreos/etcd/raft"
)

func TestSingleServer(t *testing.T) {
	log.Println("TEST >>>>>TestSingleServer<<<<")

	srv := StartServer(":1234", 1)

	argP := PutArgs{Key: "test1", Value: "v1"}
	var reP PutReply
	err := srv.Put(&argP, &reP)
	if err != nil {
		t.Error("Error happen on ", err)
	}
	log.Println(">>", reP, err)

	argP = PutArgs{Key: "test1", Value: "v2"}
	err = srv.Put(&argP, &reP)
	if err != nil {
		t.Error("Error happen on ", err)
	}
	log.Println(">>", reP, err)

	argP = PutArgs{Key: "test1", Value: "v3"}
	err = srv.Put(&argP, &reP)
	if err != nil {
		t.Error("Error happen on ", err)
	}
	log.Println(">>", reP, err)

	log.Printf("** Sleeping to visualize heartbeat between nodes **\n")
	time.Sleep(2000 * time.Millisecond)

	argG := GetArgs{Key: "test1"}
	var reG GetReply
	err = srv.Get(&argG, &reG)
	if err != nil || reG.Value != "v3" {
		t.Error("Error happen on ", err, reG)
	}

	log.Println(">>", reG, err)
	log.Println("Stop server..")
	srv.kill()
}

func TestTwoServers(t *testing.T) {
	log.Println("TEST >>>>>TestTwoServers<<<<")

	serverList := []raft.Peer{{ID: uint64(1)}, {ID: uint64(2)}}
	srv1 := StartClusterServers(":1234", 1, serverList)
	srv2 := StartClusterServers(":1235", 2, serverList)

	argP := PutArgs{Key: "test1", Value: "v1"}
	var reP PutReply
	err := srv1.Put(&argP, &reP)
	if err != nil {
		t.Error("Error happen on ", err)
	}
	log.Println(">>", reP, err)

	argP = PutArgs{Key: "test1", Value: "v2"}
	err = srv2.Put(&argP, &reP)
	if err != nil {
		t.Error("Error happen on ", err)
	}
	log.Println(">>", reP, err)

	argP = PutArgs{Key: "test1", Value: "v3"}
	err = srv1.Put(&argP, &reP)
	if err != nil {
		t.Error("Error happen on ", err)
	}
	log.Println(">>", reP, err)

	log.Printf("** Sleeping to visualize heartbeat between nodes **\n")
	time.Sleep(3000 * time.Millisecond)

	argG := GetArgs{Key: "test1"}
	var reG GetReply
	err = srv1.Get(&argG, &reG)
	if err != nil || reG.Value != "v3" {
		t.Error("Error happen on ", err, reG)
	}

	log.Println(">>", reG, err)
	log.Println("Stop server..")

	srv1.kill()
	srv2.kill()
}
