package rs

import (
	"log"
	"testing"
	"time"
)

func TestSingleServer(t *testing.T) {
	srv := StartServer(socketPort("srv", 1), 1)

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
}
