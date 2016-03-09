package rs

import (
	"fmt"
	"testing"
)

func TestServer(t *testing.T) {
	srv := StartServer(socketPort("srv", 1), 1)

	argP := PutArgs{Key: "test1", Value: "v1"}
	var reP PutReply
	srv.Put(&argP, &reP)
	fmt.Println(reP)

	argG := GetArgs{Key: "test1"}
	var reG GetReply
	srv.Get(&argG, &reG)

	fmt.Println(reG)
}
