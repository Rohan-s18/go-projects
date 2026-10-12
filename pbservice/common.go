package pbservice

import (
	"coms4113/hw2/src/viewservice"
	"hash/fnv"
)

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
)

type Err string

type PutArgs struct {
	Key     string
	Value   string
	DoHash  bool
	Request int64
}

type PutReply struct {
	Err           Err
	PreviousValue string
}

type GetArgs struct {
	Key string
}

type GetReply struct {
	Err   Err
	Value string
}

type ForwardPutArgs struct {
	Args    PutArgs
	Viewnum uint
	Primary string
}

type ForwardGetArgs struct {
	Args    GetArgs
	Viewnum uint
	Primary string
}

type TransferArgs struct {
	View    viewservice.View
	Data    map[string]string
	Results map[int64]PutReply
}

type TransferReply struct {
	Err Err
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
