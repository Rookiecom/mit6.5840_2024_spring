package kvsrv

import (
	"crypto/rand"
	"math/big"

	"6.5840/labrpc"
	"github.com/google/uuid"
)

type Clerk struct {
	server *labrpc.ClientEnd
	// You will have to modify this struct.
	CID string
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	// You'll have to add code here.
	ck.CID = uuid.NewString()
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	getReq := GetArgs{
		Key: key,
		ID:  uuid.NewString(),
		CID: ck.CID,
	}
	getReply := GetReply{}

	for {
		ok := ck.server.Call("KVServer.Get", &getReq, &getReply)

		if ok {
			ck.DeleteHistory()
			break
		}

	}

	// You will have to modify this function.
	return getReply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	// You will have to modify this function.

	req := PutAppendArgs{
		Key:   key,
		Value: value,
		ID:    uuid.NewString(),
		CID:   ck.CID,
	}

	reply := PutAppendReply{}

	for {
		ok := ck.server.Call("KVServer."+op, &req, &reply)
		if ok {
			ck.DeleteHistory()
			break
		}
	}

	return reply.Value
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) DeleteHistory() {
	req := DeleteHistoryArgs{
		CID: ck.CID,
	}
	reply := DeleteHistoryReply{}

	for {
		ok := ck.server.Call("KVServer.DeleteHistory", &req, &reply)
		if ok {
			break
		}
	}
}
