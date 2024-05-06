package kvsrv

import (
	"fmt"
	"log"
	"runtime"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func print_mem() {
	runtime.GC()
	var st runtime.MemStats
	runtime.ReadMemStats(&st)
	m := st.HeapAlloc / (1 << 20)

	fmt.Printf("%v mb\n", m)
}

type rpcHistory struct {
	ID    string
	Value string
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	kvmap map[string]string

	histroy map[string]rpcHistory
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()

	defer kv.mu.Unlock()

	if kv.histroy[args.CID].ID == args.ID {
		reply.Value = kv.histroy[args.CID].Value
		return
	}

	replaceHistory := rpcHistory{
		ID:    args.ID,
		Value: kv.kvmap[args.Key],
	}
	reply.Value = replaceHistory.Value

	kv.histroy[args.CID] = replaceHistory

}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.histroy[args.CID].ID == args.ID {
		reply.Value = kv.histroy[args.CID].Value
		return
	}
	replaceHistory := rpcHistory{
		ID:    args.ID,
		Value: kv.kvmap[args.Key],
	}
	reply.Value = replaceHistory.Value
	kv.histroy[args.CID] = replaceHistory

	kv.kvmap[args.Key] = args.Value

}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()

	defer kv.mu.Unlock()

	if kv.histroy[args.CID].ID == args.ID {
		reply.Value = kv.histroy[args.CID].Value
		return
	}
	replaceHistory := rpcHistory{
		ID:    args.ID,
		Value: kv.kvmap[args.Key],
	}
	reply.Value = replaceHistory.Value
	kv.histroy[args.CID] = replaceHistory

	kv.kvmap[args.Key] = kv.kvmap[args.Key] + args.Value
}

func (kv *KVServer) DeleteHistory(args *DeleteHistoryArgs, reply *DeleteHistoryReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	delete(kv.histroy, args.CID)
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.

	kv.kvmap = make(map[string]string)

	kv.histroy = make(map[string]rpcHistory)
	return kv
}
