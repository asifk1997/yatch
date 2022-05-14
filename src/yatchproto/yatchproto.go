package yatchproto

import (
	"state"
)

type Prepare struct {
	LeaderId int32
	Replica  int32
	Instance int32
	Ballot   int32
}

type PrepareReply struct {
	AcceptorId int32
	Replica    int32
	Instance   int32
	OK         uint8
	Ballot     int32
	Status     int8
	Command    []state.Command
	Seq        int32
	Deps       [5]int32
}

type PreAccept struct {
	LeaderId    int32
	Replica     int32
	LenCurrent  int32
	LenPrevious int32
	Current     []state.TransferLog
	Previous    []state.TransferLog
	Value       int32
	CommitIndex int32
}

type PreAcceptOK struct {
	LeaderId    int32
	Replica     int32
	LenCurrent  int32
	LenPrevious int32
	Current     []state.TransferLog
	Previous    []state.TransferLog
	Value       int32
	CommitIndex int32
}

type PreAcceptNOK struct {
	LeaderId    int32
	Replica     int32
	Value       int32
	CommitIndex int32
}

//type PreAccept struct {
//	LeaderId int32
//	Replica  int32
//	Instance int32
//	Ballot   int32
//	Command  []state.Command
//	Seq      int32
//	Deps     [5]int32
//}

type PreAcceptReply struct {
	Replica       int32
	Instance      int32
	OK            uint8
	Ballot        int32
	Seq           int32
	Deps          [5]int32
	CommittedDeps [5]int32
}

type Accept struct {
	LeaderId    int32
	Replica     int32
	Value       int32
	CommitIndex int32
}

type AcceptReply struct {
	LeaderId    int32
	Replica     int32
	Value       int32
	CommitIndex int32
}

type Commit struct {
	LeaderId int32
	Replica  int32
	Instance int32
	Command  []state.Command
	Seq      int32
	Deps     [5]int32
}

type CommitShort struct {
	LeaderId int32
	Replica  int32
	Instance int32
	Count    int32
	Seq      int32
	Deps     [5]int32
}

type TryPreAccept struct {
	LeaderId int32
	Replica  int32
	Instance int32
	Ballot   int32
	Command  []state.Command
	Seq      int32
	Deps     [5]int32
}

type TryPreAcceptReply struct {
	AcceptorId       int32
	Replica          int32
	Instance         int32
	OK               uint8
	Ballot           int32
	ConflictReplica  int32
	ConflictInstance int32
	ConflictStatus   int8
}

const (
	NONE int8 = iota
	PROPOSE
	PREACCEPTED
	PREACCEPTED_EQ
	ACCEPTED
	COMMITTED
	EXECUTED
)
