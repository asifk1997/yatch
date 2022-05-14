package yatch

import (
	"dlog"
	"fastrpc"
	"genericsmr"
	"genericsmrproto"
	"log"
	"math"
	"os"
	"state"
	"strconv"
	"sync"
	"time"
	"yatchproto"
)

const MAX_DEPTH_DEP = 10
const TRUE = uint8(1)
const FALSE = uint8(0)
const ADAPT_TIME_SEC = 10

const MAX_BATCH = 1000

const DS = 3

type Exec struct {
	r *Replica
}

type Log struct {
	Status     int8
	LengthCmds int32
	Cmds       []state.Command
	Value      int32
	lb         *LeaderBookkeeping
}

type LeaderBookkeeping struct {
	clientProposals []*genericsmr.Propose
	prepareOKs      int
	preAcceptOKs    int
	acceptOKs       int
}

type Replica struct {
	*genericsmr.Replica
	prepareChan           chan fastrpc.Serializable
	preAcceptChan         chan fastrpc.Serializable
	acceptChan            chan fastrpc.Serializable
	commitChan            chan fastrpc.Serializable
	commitShortChan       chan fastrpc.Serializable
	prepareReplyChan      chan fastrpc.Serializable
	preAcceptReplyChan    chan fastrpc.Serializable
	preAcceptOKChan       chan fastrpc.Serializable
	preAcceptNOKChan      chan fastrpc.Serializable
	acceptReplyChan       chan fastrpc.Serializable
	tryPreAcceptChan      chan fastrpc.Serializable
	tryPreAcceptReplyChan chan fastrpc.Serializable
	prepareRPC            uint8
	prepareReplyRPC       uint8
	preAcceptRPC          uint8
	preAcceptReplyRPC     uint8
	preAcceptOKRPC        uint8
	preAcceptNOKRPC       uint8
	acceptRPC             uint8
	acceptReplyRPC        uint8
	commitRPC             uint8
	commitShortRPC        uint8
	tryPreAcceptRPC       uint8
	tryPreAcceptReplyRPC  uint8
	logs                  []*Log
	CommittedIndex        int32   // highest committed instance per replica that this replica knows about
	ExecedUpTo            []int32 // instance up to which all commands have been executed (including itself)
	exec                  *Exec
	conflicts             []map[state.Key]int32
	clientMutex           *sync.Mutex // for synchronizing when sending replies to clients from multiple go-routines
	repliesCount          map[int32]int32
	maxCommandValue       int32
	currRound             int32
}

type RecievedMessage struct {
	preAccept   *yatchproto.PreAccept
	accept      *yatchproto.Accept
	isPreAccept bool
}

func NewReplica(id int, peerAddrList []string, thrifty bool, exec bool, dreply bool, beacon bool, durable bool) *Replica {
	peers := make([]string, 3)
	r := &Replica{genericsmr.NewReplica(id, peerAddrList, thrifty, exec, dreply),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE*3),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE*3),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE*3),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE*3),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE*3),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE*2),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
		make([]*Log, 1024*len(peers)),
		-1,
		make([]int32, len(peers)),
		nil,
		make([]map[state.Key]int32, len(peers)),
		new(sync.Mutex),
		make(map[int32]int32),
		-1,
		0,
	}
	r.Beacon = beacon
	r.Durable = durable

	for i := 0; i < r.N; i++ {
		r.ExecedUpTo[i] = -1
	}

	r.exec = &Exec{r}

	//cpMarker = make([]state.Command, 0)

	//register RPCs
	r.prepareRPC = r.RegisterRPC(new(yatchproto.Prepare), r.prepareChan)
	r.prepareReplyRPC = r.RegisterRPC(new(yatchproto.PrepareReply), r.prepareReplyChan)
	r.preAcceptRPC = r.RegisterRPC(new(yatchproto.PreAccept), r.preAcceptChan)
	r.preAcceptReplyRPC = r.RegisterRPC(new(yatchproto.PreAcceptReply), r.preAcceptReplyChan)
	r.preAcceptOKRPC = r.RegisterRPC(new(yatchproto.PreAcceptOK), r.preAcceptOKChan)
	r.preAcceptNOKRPC = r.RegisterRPC(new(yatchproto.PreAcceptNOK), r.preAcceptNOKChan)
	r.acceptRPC = r.RegisterRPC(new(yatchproto.Accept), r.acceptChan)
	r.acceptReplyRPC = r.RegisterRPC(new(yatchproto.AcceptReply), r.acceptReplyChan)
	r.commitRPC = r.RegisterRPC(new(yatchproto.Commit), r.commitChan)
	r.commitShortRPC = r.RegisterRPC(new(yatchproto.CommitShort), r.commitShortChan)
	r.tryPreAcceptRPC = r.RegisterRPC(new(yatchproto.TryPreAccept), r.tryPreAcceptChan)
	r.tryPreAcceptReplyRPC = r.RegisterRPC(new(yatchproto.TryPreAcceptReply), r.tryPreAcceptReplyChan)

	r.ProposeCommand(id, 0, 0)
	r.ProposeCommand(id, 0, 1)
	r.ProposeCommand(id, 0, 2)
	go r.run()

	return r
}

//func (r *Replica) MakeInstance(q int, round int32, value int32) {
//	command := state.Command{state.PUT, state.Key(value), state.Value(value)}
//	cmds := []state.Command{}
//	cmds = append(cmds, command)
//	dlog.Println(cmds)
//	r.logs[value] = &Log{Cmds: cmds, Round: round, Value: value, Status: yatchproto.COMMITTED}
//}

func (r *Replica) ProposeCommand(q int, round int32, value int32) {
	command := state.Command{state.PUT, state.Key(value), state.Value(value)}
	cmds := []state.Command{}
	cmds = append(cmds, command)
	//lb := &LeaderBookkeeping{nil, 0, 0, 0}
	r.logs[value] = &Log{LengthCmds: int32(len(cmds)), Status: yatchproto.PROPOSE, Cmds: cmds, Value: value}
	r.maxCommandValue = int32(math.Max(float64(r.maxCommandValue), float64(value)))
}

func (r *Replica) ProposeCommands(round int32, value int32, cmds []state.Command, bookkeeping *LeaderBookkeeping) {
	r.logs[value] = &Log{LengthCmds: int32(len(cmds)), Status: yatchproto.PROPOSE, Cmds: cmds, Value: value, lb: bookkeeping}
}

//func (r *Replica) handleMessage(m Message, r0 *Replica, r1 *Replica, r2 *Replica) {
//	prevRound := int32(0)
//	if m.Value > r.maxCommandValue {
//		prevRound = r.maxCommandValue / int32(r.N)
//	} else {
//		prevRound = m.Value/int32(r.N) - 1
//	}
//
//	prevRoundStart := prevRound * int32(r.N)
//	prevRoundEnd := prevRound*int32(r.N) + int32(r.N)
//
//	logsOfPreviousRound := r.logs[prevRoundStart:prevRoundEnd]
//	if previousRoundLogsSame(logsOfPreviousRound, m.Previous) {
//		if r.CommittedIndex == m.CommitIndex {
//			round := int32(prevRound) + 1
//			if r.checkCommitIndex(int(m.Value)) {
//				//dlog.Println("checkCommitIndex")
//				r.incrementCommitIndex(prevRound)
//			}
//			r.insertLogsOfMessageToCurrentRound(m, round)
//		} else {
//			//dlog.Println("commitIndex not same r", r.Id, m.CommitIndex, r.CommittedIndex)
//			if m.CommitIndex > r.CommittedIndex {
//				r.incrementCommitIndex(prevRound)
//			}
//		}
//	} else {
//		//dlog.Println("previous round of logs not same", logsOfPreviousRound, m.Previous)
//	}
//}

func (r *Replica) insertLogsOfMessageToCurrentRound(m RecievedMessage, round int32) {
	if m.isPreAccept == true {
		dlog.Println("insertLogsOfMessageToCurrentRoundPreAccept", round, m.preAccept.Current)
		dlog.Println("insert", "LeaderId", m.preAccept.LeaderId)
		dlog.Println("insert", "Replica", m.preAccept.Replica)
		dlog.Println("insert", "LenCurrent", m.preAccept.LenCurrent)
		dlog.Println("insert", "LenPrevious", m.preAccept.LenPrevious)
		dlog.Println("insert", "Current", m.preAccept.Current)
		dlog.Println("insert", "Previous", m.preAccept.Previous)
		dlog.Println("insert", "Value", m.preAccept.Value)
		dlog.Println("insert", "CommitIndex", m.preAccept.CommitIndex)
	} else {
		dlog.Println("insert", "LeaderId", m.accept.LeaderId)
		dlog.Println("insert", "Replica", m.accept.Replica)
		dlog.Println("insert", "Value", m.accept.Value)
		dlog.Println("insert", "CommitIndex", m.accept.CommitIndex)
	}
	startIndex := round * int32(r.N)
	endIndex := round*int32(r.N) + int32(r.N)

	dlog.Println("before")
	for i := startIndex; i < endIndex; i++ {
		dlog.Println(r.logs[i])
	}

	for i := startIndex; i < endIndex; i++ {
		if r.logs[i] != nil {
			continue
		}
		length := int32(0)
		if m.isPreAccept == true {
			length = m.preAccept.Current[i-startIndex].LengthCmds
		}

		if length > 0 {
			l := &Log{}
			if m.isPreAccept == true {
				l = &Log{
					Cmds:       m.preAccept.Current[i-startIndex].Cmds,
					Status:     m.preAccept.Current[i-startIndex].Status,
					LengthCmds: m.preAccept.Current[i-startIndex].LengthCmds,
					Value:      m.preAccept.Current[i-startIndex].Value,
				}
			}

			r.logs[i] = l
			r.maxCommandValue = int32(math.Max(float64(r.maxCommandValue), float64(l.Value)))
			r.currRound = r.maxCommandValue / int32(r.N)
		}
	}

	dlog.Println("after")
	for i := startIndex; i < endIndex; i++ {
		dlog.Println(r.logs[i])
	}

}

func (r *Replica) insertLogsOfMessageToPreviousRoundIndexWise(m RecievedMessage, startIndex int32, endIndex int32) {
	if m.isPreAccept == true {
		dlog.Println("insertLogsOfMessageToCurrentRoundIndexWise", startIndex, endIndex, m.preAccept.Current)
		dlog.Println("insert", "LeaderId", m.preAccept.LeaderId)
		dlog.Println("insert", "Replica", m.preAccept.Replica)
		dlog.Println("insert", "LenCurrent", m.preAccept.LenCurrent)
		dlog.Println("insert", "LenPrevious", m.preAccept.LenPrevious)
		dlog.Println("insert", "Current", m.preAccept.Current)
		dlog.Println("insert", "Previous", m.preAccept.Previous)
		dlog.Println("insert", "Value", m.preAccept.Value)
		dlog.Println("insert", "CommitIndex", m.preAccept.CommitIndex)
	} else {
		dlog.Println("insert", "LeaderId", m.accept.LeaderId)
		dlog.Println("insert", "Replica", m.accept.Replica)
		dlog.Println("insert", "Value", m.accept.Value)
		dlog.Println("insert", "CommitIndex", m.accept.CommitIndex)
	}

	dlog.Println("before")
	for i := startIndex; i < endIndex; i++ {
		dlog.Println(r.logs[i])
	}

	for i := startIndex; i < endIndex; i++ {
		if r.logs[i] != nil {
			continue
		}
		length := int32(0)
		if m.isPreAccept == true {
			length = m.preAccept.Previous[i-startIndex].LengthCmds
		}

		if length > 0 {
			l := &Log{}
			if m.isPreAccept == true {
				l = &Log{
					Cmds:       m.preAccept.Previous[i-startIndex].Cmds,
					Status:     m.preAccept.Previous[i-startIndex].Status,
					LengthCmds: m.preAccept.Previous[i-startIndex].LengthCmds,
					Value:      m.preAccept.Previous[i-startIndex].Value,
				}
			}

			r.logs[i] = l
			r.maxCommandValue = int32(math.Max(float64(r.maxCommandValue), float64(l.Value)))
			r.currRound = r.maxCommandValue / int32(r.N)
		}
	}

	dlog.Println("after")
	for i := startIndex; i < endIndex; i++ {
		dlog.Println(r.logs[i])
	}

}

func (r *Replica) checkCommitIndex(value int32) bool {
	//dlog.Println("checkCommitIndex")
	if r.repliesCount[value] < int32(r.N)/2 {
		return false
	} else {
		return true
	}
}

func (r *Replica) incrementCommitIndex(round int32) {
	initCommitIndex := r.CommittedIndex
	r.CommittedIndex = round*int32(r.N) + int32(r.N) - 1
	updatedCommitIndex := r.CommittedIndex
	dlog.Println("updated commitIndex", r.CommittedIndex, "at replica", r.Id)
	for i := initCommitIndex + 1; i <= updatedCommitIndex; i++ {
		if r.logs[i] != nil {
			r.logs[i].Status = yatchproto.COMMITTED
		}
	}
}

func (r *Replica) incrementCommitIndexRange(start, end int32) {
	initCommitIndex := r.CommittedIndex
	updatedCommitIndex := end - 1
	dlog.Println("updated commitIndex", r.CommittedIndex, "at replica", r.Id)
	for i := initCommitIndex + 1; i <= updatedCommitIndex; i++ {
		if r.logs[i] != nil {
			r.logs[i].Status = yatchproto.COMMITTED
		}
	}
}

func (r *Replica) execute(replica int32, round int32) {
	dlog.Println("execute")
	startIndex := round * int32(r.N)
	endIndex := round*int32(r.N) + int32(r.N)
	dlog.Println("start", startIndex, "end", endIndex)
	for i := int32(startIndex); i < endIndex; i++ {
		dlog.Println("index", i)
		if r.logs[i] == nil || r.logs[i].Cmds == nil {
			dlog.Println("continue1", i)
			continue
		}
		if r.logs[i].Status == yatchproto.EXECUTED {
			dlog.Println("continue2")
			continue
		}
		for r.logs[i].Status != yatchproto.COMMITTED {
			dlog.Println("sleeping1")
			time.Sleep(1000 * 1000)
		}

		w := r.logs[i]
		dlog.Println("w", w)
		for w.Cmds == nil {
			dlog.Println("sleeping2")
			time.Sleep(1000 * 1000)
		}
		for idx := 0; idx < len(w.Cmds); idx++ {
			val := w.Cmds[idx].Execute(r.State)
			dlog.Println(val)
		}
		w.Status = yatchproto.EXECUTED
	}
	dlog.Println("executed ", round)
}

func (r *Replica) ExecuteCommand(replica int32, round int32) bool {
	dlog.Println("executeCommand")
	roundStart := round * int32(r.N)
	roundEnd := round*int32(r.N) + int32(r.N)
	logsForCurrentRound := r.logs[roundStart:roundEnd]
	dlog.Println(r.logs[roundStart])
	dlog.Println(r.logs[roundEnd])
	dlog.Println(logsForCurrentRound)
	r.execute(replica, round)
	return true
}

func (r *Replica) visualise(round int32) {
	dlog.Println("visualise replica ", r.Id, " round ", round)
	for i := int32(0); i <= round; i++ {
		for q := int32(0); q < int32(r.N); q++ {
			inst := r.logs[i*int32(r.N)+q]
			dlog.Printf("vis ", inst)
		}
		dlog.Println()
	}
}

//sync with the stable store
func (r *Replica) sync() {
	if !r.Durable {
		return
	}

	r.StableStore.Sync()
}

/* Clock goroutine */

var fastClockChan chan bool
var slowClockChan chan bool

func (r *Replica) fastClock() {
	for !r.Shutdown {
		time.Sleep(1 * 1e7) // 5 ms
		fastClockChan <- true
	}
}
func (r *Replica) slowClock() {
	for !r.Shutdown {
		time.Sleep(150 * 1e6) // 150 ms
		slowClockChan <- true
	}
}

func (r *Replica) handlePropose(propose *genericsmr.Propose) {
	//TODO!! Handle client retries
	dlog.Println("handlePropose Yatch")

	batchSize := len(r.ProposeChan) + 1
	if batchSize > MAX_BATCH {
		batchSize = MAX_BATCH
	}

	instNo := int32(0)
	//r.crtInstance[r.Id]++
	cmds := make([]state.Command, batchSize)
	proposals := make([]*genericsmr.Propose, batchSize)
	cmds[0] = propose.Command
	proposals[0] = propose
	for i := 1; i < batchSize; i++ {
		prop := <-r.ProposeChan
		cmds[i] = prop.Command
		proposals[i] = prop
	}
	dlog.Println("cmds", cmds)
	r.startPhase1(r.Id, instNo, 0, proposals, cmds, batchSize)
}

//func main() {
//
//	r := initReplica(0)
//
//	r.MakeInstance(0, 0, 0)
//	r.MakeInstance(1, 0, 1)
//	r.MakeInstance(2, 0, 2)
//
//	r.MakeInstance(0, 1, 3)
//	r.MakeInstance(1, 1, 4)
//	r.MakeInstance(2, 1, 5)
//
//	r.MakeInstance(0, 2, 6)
//	r.MakeInstance(1, 2, 7)
//	r.MakeInstance(2, 2, 8)
//
//	r.MakeInstance(0, 3, 9)
//	r.MakeInstance(1, 3, 10)
//	r.MakeInstance(2, 3, 11)
//
//	r.MakeInstance(0, 4, 12)
//	r.MakeInstance(1, 4, 13)
//	r.MakeInstance(2, 4, 14)
//
//	r.MakeInstance(0, 5, 15)
//	r.MakeInstance(1, 5, 16)
//	r.MakeInstance(2, 5, 17)
//	//r.visualise(2)
//	start := time.Now()
//	//r.exec.visualise(5)
//	r.ExecuteCommand(0, 5)
//	elapsed := time.Since(start)
//	//r.exec.visualise(0, 5)
//	r.ExecuteCommand(0, 5)
//	elapsed2 := time.Since(start)
//	log.Printf("Yatch took %s", elapsed)
//	log.Printf("Yatch took %s", elapsed2)
//	fmt.Println("Test ended\n")
//}

func (r *Replica) stopAdapting() {
	time.Sleep(1000 * 1000 * 1000 * ADAPT_TIME_SEC)
	r.Beacon = false
	time.Sleep(1000 * 1000 * 1000)

	for i := 0; i < r.N-1; i++ {
		min := i
		for j := i + 1; j < r.N-1; j++ {
			if r.Ewma[r.PreferredPeerOrder[j]] < r.Ewma[r.PreferredPeerOrder[min]] {
				min = j
			}
		}
		aux := r.PreferredPeerOrder[i]
		r.PreferredPeerOrder[i] = r.PreferredPeerOrder[min]
		r.PreferredPeerOrder[min] = aux
	}

	log.Println(r.PreferredPeerOrder)
}

var conflicted, weird, slow, happy int

/* ============= */

/***********************************
   Main event processing loop      *
************************************/

var onOffProposeChan chan *genericsmr.Propose
var cnt int

func (r *Replica) run() {
	dlog.Println("RID", r.Id)
	dlog.Println("RID", string(r.Id))
	f, err := os.OpenFile("test-logfile"+strconv.Itoa(int(r.Id))+".txt", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	defer func(f *os.File) {
		err := f.Close()
		if err != nil {
			log.Fatalf("error closing file")
		}
	}(f)
	cnt = 0
	//log.SetOutput(f)
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.Println("This is a test log entry")

	dlog.Println("Yatch replica run")
	r.ConnectToPeers()

	dlog.Println("Waiting for client connections")

	go r.WaitForClientConnections()

	//if r.Exec {
	//	go r.executeCommands()
	//}

	if r.Id == 0 {
		//init quorum read lease
		quorum := make([]int32, r.N/2+1)
		for i := 0; i <= r.N/2; i++ {
			quorum[i] = int32(i)
		}
		r.UpdatePreferredPeerOrder(quorum)
	}

	slowClockChan = make(chan bool, 1)
	fastClockChan = make(chan bool, 1)
	go r.slowClock()

	//Enabled when batching for 5ms
	if MAX_BATCH > 100 {
		go r.fastClock()
	}

	if r.Beacon {
		go r.stopAdapting()
	}

	//onOffProposeChan := r.ProposeChan
	onOffProposeChan = r.ProposeChan
	for !r.Shutdown {

		select {

		case propose := <-onOffProposeChan:
			//got a Propose from a client
			dlog.Printf("Proposal with op %d\n", propose.Command.Op)
			//r.clientMutex.Lock()
			r.handlePropose(propose)
			//r.clientMutex.Unlock()
			//deactivate new proposals channel to prioritize the handling of other protocol messages,
			//and to allow commands to accumulate for batching
			onOffProposeChan = nil
			dlog.Println("closed channel on proposing")
			break

		case <-fastClockChan:
			//activate new proposals channel
			prevRound := r.CommittedIndex / int32(r.N)
			if onOffProposeChan == nil && r.CommittedIndex == -1 {
				dlog.Println("breaking out")
				break
			}
			//dlog.Println("print ")
			if onOffProposeChan == nil && cnt < 10 {
				dlog.Println("r.currRound", r.currRound, "prevRound", prevRound)
				cnt++
			}
			if onOffProposeChan == nil && r.currRound == prevRound+1 {
				log.Println("activating propose channel", "prevRound", prevRound, "currRound", r.currRound, "commitedIndex", r.CommittedIndex)
				log.Println("r.msgCount", r.MsgCount)
				onOffProposeChan = r.ProposeChan
			}
			break

		//case prepareS := <-r.prepareChan:
		//	prepare := prepareS.(*yatchproto.Prepare)
		//	//got a Prepare message
		//	dlog.Printf("Received Prepare for instance %d.%d\n", prepare.Replica, prepare.Instance)
		//	//r.handlePrepare(prepare)
		//	break

		case preAcceptS := <-r.preAcceptChan:
			preAccept := preAcceptS.(*yatchproto.PreAccept)
			//got a PreAccept message
			dlog.Printf("Received PreAccept for value %d.%d\n", preAccept.LeaderId, preAccept.Value)
			onOffProposeChan = nil
			dlog.Println("closed channel on recieving preAccept")
			r.handlePreAccept(preAccept)
			break

		case acceptS := <-r.acceptChan:
			accept := acceptS.(*yatchproto.Accept)
			//got an Accept message
			dlog.Printf("Received Accept for value %d.%d from %d\n", accept.LeaderId, accept.Value, accept.Replica)
			//onOffProposeChan = nil
			//dlog.Println("closed channel on recieving accept")
			r.handleAccept(accept)
			break

		//case commitS := <-r.commitChan:
		//	commit := commitS.(*yatchproto.Commit)
		//	//got a Commit message
		//	dlog.Printf("Received Commit for instance %d.%d\n", commit.LeaderId, commit.Instance)
		//	//r.handleCommit(commit)
		//	break
		//
		//case commitS := <-r.commitShortChan:
		//	commit := commitS.(*yatchproto.CommitShort)
		//	//got a Commit message
		//	dlog.Printf("Received Commit for instance %d.%d\n", commit.LeaderId, commit.Instance)
		//	//r.handleCommitShort(commit)
		//	break
		//
		//case prepareReplyS := <-r.prepareReplyChan:
		//	prepareReply := prepareReplyS.(*yatchproto.PrepareReply)
		//	//got a Prepare reply
		//	dlog.Printf("Received PrepareReply for instance %d.%d\n", prepareReply.Replica, prepareReply.Instance)
		//	//r.handlePrepareReply(prepareReply)
		//	break
		//
		//case preAcceptReplyS := <-r.preAcceptReplyChan:
		//	preAcceptReply := preAcceptReplyS.(*yatchproto.PreAcceptReply)
		//	//got a PreAccept reply
		//	dlog.Printf("Received PreAcceptReply for instance %d.%d\n", preAcceptReply.Replica, preAcceptReply.Instance)
		//	//r.handlePreAcceptReply(preAcceptReply)
		//	break
		//
		case preAcceptOKS := <-r.preAcceptOKChan:
			preAcceptOK := preAcceptOKS.(*yatchproto.PreAcceptOK)
			//got a PreAccept reply
			dlog.Printf("Received PreAcceptOK for replica,commitIndex %d.%d from %d\n", r.Id, preAcceptOK.CommitIndex, preAcceptOK.Replica)
			r.handlePreAcceptOK(preAcceptOK)
			break

		case preAcceptNOKS := <-r.preAcceptNOKChan:
			preAcceptNOK := preAcceptNOKS.(*yatchproto.PreAcceptNOK)
			//got a PreAccept reply
			dlog.Printf("Received PreAcceptNOK for replica,commitIndex %d.%d from %d\n", r.Id, preAcceptNOK.CommitIndex, preAcceptNOK.Replica)
			r.handlePreAcceptNOK(preAcceptNOK)
			break
		case acceptReplyS := <-r.acceptReplyChan:
			acceptReply := acceptReplyS.(*yatchproto.AcceptReply)
			//got an Accept reply
			dlog.Printf("Received AcceptReply for instance %d.%d\n", acceptReply.Replica, acceptReply.Value)
			r.handleAcceptReply(acceptReply)
			//if r.logs[acceptReply.Value].lb.acceptOKs >= r.N/2 {
			//	onOffProposeChan = r.ProposeChan
			//}
			break
		//
		//case tryPreAcceptS := <-r.tryPreAcceptChan:
		//	tryPreAccept := tryPreAcceptS.(*yatchproto.TryPreAccept)
		//	dlog.Printf("Received TryPreAccept for instance %d.%d\n", tryPreAccept.Replica, tryPreAccept.Instance)
		//	//r.handleTryPreAccept(tryPreAccept)
		//	break
		//
		//case tryPreAcceptReplyS := <-r.tryPreAcceptReplyChan:
		//	tryPreAcceptReply := tryPreAcceptReplyS.(*yatchproto.TryPreAcceptReply)
		//	dlog.Printf("Received TryPreAcceptReply for instance %d.%d\n", tryPreAcceptReply.Replica, tryPreAcceptReply.Instance)
		//	//r.handleTryPreAcceptReply(tryPreAcceptReply)
		//	break

		case beacon := <-r.BeaconChan:
			dlog.Printf("Received Beacon from replica %d with timestamp %d\n", beacon.Rid, beacon.Timestamp)
			//r.ReplyBeacon(beacon)
			break

		case <-slowClockChan:
			//dlog.Println("slowclockchannel")
			if r.Beacon {
				for q := int32(0); q < int32(r.N); q++ {
					if q == r.Id {
						continue
					}
					r.SendBeacon(q)
				}
			}
			break
		case <-r.OnClientConnect:
			log.Printf("weird %d; conflicted %d; slow %d; happy %d\n", weird, conflicted, slow, happy)
			weird, conflicted, slow, happy = 0, 0, 0, 0
			//
			//case iid := <-r.instancesToRecover:
			//r.startRecoveryForInstance(iid.replica, iid.instance)
		}
	}
}

func (r *Replica) startPhase1(replica int32, instance int32, ballot int32, proposals []*genericsmr.Propose, cmds []state.Command, batchSize int) {
	//init command attributes
	dlog.Println("startPhase1")
	//TODO: logic for next round value
	nextRound := r.maxCommandValue/int32(r.N) + 1
	nextValue := nextRound*int32(r.N) + int32(r.Id)

	lb := &LeaderBookkeeping{proposals, 0, 0, 0}
	r.ProposeCommands(nextRound, nextValue, cmds, lb)
	r.maxCommandValue = int32(math.Max(float64(nextValue), float64(r.maxCommandValue)))
	r.currRound = r.maxCommandValue / int32(r.N)
	currentRoundStart := nextRound * int32(r.N)
	currentRoundEnd := nextRound*int32(r.N) + int32(r.N)
	r.currRound = nextRound
	dlog.Println("r.currRound", r.currRound)
	dlog.Println("currentRoundStart", currentRoundStart, "currentRoundEnd", currentRoundEnd)
	currentRoundOfLogs := r.getTransferLogs(currentRoundStart, currentRoundEnd)
	//r.logs[currentRoundStart:currentRoundEnd]
	previousRound := nextRound - 1
	previousRoundStart := previousRound * int32(r.N)
	previousRoundEnd := previousRound*int32(r.N) + int32(r.N)
	previousRoundOfLogs := r.getTransferLogs(previousRoundStart, previousRoundEnd)
	if true {
		r.bcastPreAccept(r.Id, nextValue, currentRoundOfLogs, previousRoundOfLogs, r.CommittedIndex)
	}

}

func (r *Replica) printMessageArgs(rm RecievedMessage) {
	if rm.isPreAccept {
		dlog.Println("handlePreAccept Yatch")
		dlog.Println("handlePreAccept.LeaderId", rm.preAccept.LeaderId)
		dlog.Println("handlePreAccept.Replica", rm.preAccept.Replica)
		dlog.Println("handlePreAccept.LenCurrent", rm.preAccept.LenCurrent)
		dlog.Println("handlePreAccept.LenPrevious", rm.preAccept.LenPrevious)
		dlog.Println("handlePreAccept.Current", rm.preAccept.Current)
		dlog.Println("handlePreAccept.Previous", rm.preAccept.Previous)
		dlog.Println("handlePreAccept.Value", rm.preAccept.Value)
		dlog.Println("handlePreAccept.CommitIndex", rm.preAccept.CommitIndex)
	} else {
		dlog.Println("handleAccept Yatch")
		dlog.Println("handleAccept.LeaderId", rm.accept.LeaderId)
		dlog.Println("handleAccept.Replica", rm.accept.Replica)
		dlog.Println("handleAccept.Value", rm.accept.Value)
		dlog.Println("handleAccept.CommitIndex", rm.accept.CommitIndex)
	}
}

func (r *Replica) getCurrentRoundAccept(rm RecievedMessage) {
	round := int32(0)
	if rm.isPreAccept {
		round = rm.preAccept.Value / int32(r.N)
	} else {
		round = rm.accept.Value / int32(r.N)
	}
	prevRound := int32(0)
	if round == r.currRound {
		// both currRound are same
		dlog.Println("both currRound are same")
		prevRound = round - 1
	} else if round == r.currRound+1 {
		dlog.Println("nodes current round", r.currRound, "other nodes round", round)
		dlog.Println("nodes currentRound is one lower, other node has higher round")
		prevRound = r.currRound
	} else {
		dlog.Println("other node", round, "this node", r.currRound)
		dlog.Println("exception case, check for this case")
		if r.currRound == 0 {
			dlog.Println("exception1")
			prevRound = r.currRound
		} else {
			dlog.Println("exception2")
			prevRound = r.currRound
			onOffProposeChan = nil
			dlog.Println("closing channel on exception2 accept")
			//return
		}
	}
	if r.logs[rm.accept.Value] != nil {
		if rm.accept.CommitIndex > r.CommittedIndex {
			r.incrementCommitIndex(prevRound)
		}
	}

	r.replyAccept(rm.accept.LeaderId,
		&yatchproto.AcceptReply{
			rm.accept.LeaderId,
			r.Id,
			rm.accept.Value,
			r.CommittedIndex,
		})

}

func (r *Replica) getCurrentRound(rm RecievedMessage) {
	round := int32(0)
	if rm.isPreAccept {
		round = rm.preAccept.Value / int32(r.N)
	} else {
		round = rm.accept.Value / int32(r.N)
	}
	prevRound := int32(0)
	if round == r.currRound {
		// both currRound are same
		dlog.Println("both currRound are same")
		prevRound = round - 1
	} else if round == r.currRound+1 {
		dlog.Println("nodes current round", r.currRound, "other nodes round", round)
		dlog.Println("nodes currentRound is one lower, other node has higher round")
		prevRound = r.currRound
	} else {
		dlog.Println("other node", round, "this node", r.currRound)
		dlog.Println("exception case, check for this case")
		if r.currRound == 0 {
			dlog.Println("exception1")
			prevRound = r.currRound
		} else {
			dlog.Println("exception2")
			prevRound = r.currRound
			onOffProposeChan = nil
			dlog.Println("closing channel on exception2")
		}
	}
	currRoundStart := round * int32(r.N)
	currRoundEnd := round*int32(r.N) + int32(r.N)
	prevRoundStart := prevRound * int32(r.N)
	prevRoundEnd := prevRound*int32(r.N) + int32(r.N)
	logsOfPreviousRound := r.getLogsOfRound(prevRoundStart, prevRoundEnd)
	previousRoundLogsSameOrNot := true
	if rm.isPreAccept {
		previousRoundLogsSameOrNot = previousRoundLogsSame(logsOfPreviousRound, rm.preAccept.Previous)
	}

	if previousRoundLogsSameOrNot {
		msgCommitIndex := int32(0)
		if rm.isPreAccept {
			msgCommitIndex = rm.preAccept.CommitIndex
		}
		if r.CommittedIndex == msgCommitIndex {
			oneBeforePrevRound := prevRound - 1
			maxOneBeforePrevRound := oneBeforePrevRound*int32(r.N) + int32(r.N) - 1
			maxPrevRound := prevRound*int32(r.N) + int32(r.N) - 1
			if r.CommittedIndex == maxOneBeforePrevRound {
				dlog.Println("r.CommittedIndex == maxOneBeforePrevRound")
				nround := int32(prevRound) + 1
				r.insertLogsOfMessageToCurrentRound(rm, nround)
			} else if r.CommittedIndex == maxPrevRound {
				dlog.Println("r.CommittedIndex == maxPrevRound")
				nround := int32(prevRound) + 1
				r.insertLogsOfMessageToCurrentRound(rm, nround)
			}

		} else if r.CommittedIndex != msgCommitIndex {
			//dlog.Println("commitIndex not same r", r.Id, preAccept.CommitIndex, r.CommittedIndex)
			if msgCommitIndex > r.CommittedIndex {
				r.incrementCommitIndex(prevRound)
			}
			r.insertLogsOfMessageToCurrentRound(rm, round)
		}

	} else {
		dlog.Println("previous round of logs not same")
		dlog.Println(logsOfPreviousRound)
		if rm.isPreAccept {
			dlog.Println(rm.preAccept.Previous)
		}

		msgCommitIndex := int32(0)
		if rm.isPreAccept {
			msgCommitIndex = rm.preAccept.CommitIndex
		} else {
			msgCommitIndex = rm.accept.CommitIndex
		}
		if r.CommittedIndex == msgCommitIndex {
			prevR := r.CommittedIndex / int32(r.N)
			r.unionOperationOnCurrentRound(prevR+1, rm.preAccept.Previous, logsOfPreviousRound)
			r.unionOperationOnCurrentRound(prevR+2, rm.preAccept.Current, r.getLogsOfRound(currRoundStart, currRoundEnd))
			dlog.Println("Send PreAcceptOK 2")
		} else if r.CommittedIndex < msgCommitIndex {
			//todo first round matching or not
			dlog.Println("r.CommittedIndex < preAccept.CommitIndex")
			dlog.Println("r.CommitIndex", r.CommittedIndex)
			dlog.Println("preAccept.CommitIndex", msgCommitIndex)
			if len(rm.preAccept.Previous) == r.N {
				pnok := &yatchproto.PreAcceptNOK{}
				pnok.LeaderId = rm.preAccept.LeaderId
				pnok.Replica = r.Id
				pnok.Value = rm.preAccept.Value
				pnok.CommitIndex = r.CommittedIndex
				dlog.Println("rm.preAccept.LeaderId", rm.preAccept.LeaderId)
				dlog.Println("r.preAcceptNOKRPC", r.preAcceptNOKRPC)
				dlog.Println("pnok", pnok)
				r.SendMsg(rm.preAccept.LeaderId, r.preAcceptNOKRPC, pnok)
				dlog.Println("Send PreAcceptOK 3")
				return
			} else {
				// todo : apply correction
				dlog.Println("apply correction")
				r1 := r.getFirstRound(rm)
				startIndex := r1 * int32(r.N)
				endIndex := startIndex + int32(len(rm.preAccept.Previous))
				r.insertLogsOfMessageToPreviousRoundIndexWise(rm, startIndex, endIndex)
				r.incrementCommitIndexRange(startIndex, endIndex)
				rCurr := endIndex / int32(r.N)
				r.insertLogsOfMessageToCurrentRound(rm, rCurr)
				r.incrementCommitIndex(rCurr)
				dlog.Println("Send After PreAccept Correction")
			}
		} else {
			dlog.Println("r.CommittedIndex > preAccept.CommitIndex")
			dlog.Println("r.CommitIndex", r.CommittedIndex)
			dlog.Println("preAccept.CommitIndex", msgCommitIndex)
			//TODO : send message in this case
			otherNodeCommitIndex := msgCommitIndex
			otherNodeRound := otherNodeCommitIndex / int32(r.N)
			prevStartIndex := otherNodeRound * int32(r.N)
			prevEndIndex := (r.currRound-1)*int32(r.N) + int32(r.N)
			prevLogs := r.getTransferLogs(prevStartIndex, prevEndIndex)
			currStartIndex := r.currRound * int32(r.N)
			currEndIndex := r.currRound*int32(r.N) + int32(r.N)
			currLogs := r.getTransferLogs(currStartIndex, currEndIndex)

			dlog.Println("Send PreAcceptAgain")
			paa := &yatchproto.PreAccept{}
			paa.LeaderId = rm.preAccept.LeaderId
			dlog.Println("paa.LeaderId", paa.LeaderId)
			paa.Replica = r.Id
			dlog.Println("paa.Replica", paa.Replica)
			paa.LenCurrent = int32(len(currLogs))
			paa.Current = currLogs
			paa.LenPrevious = int32(len(prevLogs))
			paa.Previous = prevLogs
			paa.Value = rm.preAccept.Value
			paa.CommitIndex = r.CommittedIndex
			dlog.Println("paa", paa)
			r.SendMsg(rm.preAccept.LeaderId, r.preAcceptRPC, paa)
			dlog.Printf("I've replied to the PreAccept with preAccept\n")
			dlog.Println("Send PreAcceptOK 4")
			return
		}
	}
	//r.printCurrentState()
	if true {
		currLogs := r.getLogsOfRound(currRoundStart, currRoundEnd)
		currTransferLogs := getLogsToTransferLogs(currLogs)

		prevLogs := r.getLogsOfRound(prevRoundStart, prevRoundEnd)
		prevTransferLogs := getLogsToTransferLogs(prevLogs)
		if rm.isPreAccept {
			dlog.Println("Send PreAcceptOK")
			pok := &yatchproto.PreAcceptOK{}
			pok.LeaderId = rm.preAccept.LeaderId
			pok.Replica = r.Id
			pok.LenCurrent = int32(len(currTransferLogs))
			pok.Current = currTransferLogs
			pok.LenPrevious = int32(len(prevTransferLogs))
			pok.Previous = prevTransferLogs
			pok.Value = rm.preAccept.Value
			pok.CommitIndex = r.CommittedIndex
			dlog.Println("pok", pok)
			r.SendMsg(rm.preAccept.LeaderId, r.preAcceptOKRPC, pok)
			dlog.Printf("I've replied to the PreAccept\n")
		} else {
			dlog.Println("Send AcceptOK")
			r.replyAccept(rm.accept.LeaderId,
				&yatchproto.AcceptReply{
					rm.accept.LeaderId,
					r.Id,

					rm.accept.Value,
					r.CommittedIndex,
				})

			dlog.Printf("I've replied to the Accept\n")
		}
	}

}

func (r *Replica) getFirstRound(rm RecievedMessage) int32 {
	for i := int32(0); i < int32(r.N); i++ {
		if rm.preAccept.Previous[i].LengthCmds == 0 {
			continue
		} else {
			return rm.preAccept.Previous[i].Value / int32(r.N)
		}
	}
	dlog.Println("could not find the value")
	return 0
}

func (r *Replica) handlePreAccept(preAccept *yatchproto.PreAccept) {
	rm := RecievedMessage{preAccept, nil, true}
	r.printMessageArgs(rm)
	r.getCurrentRound(rm)
}

func getLogsToTransferLogs(curr []Log) []state.TransferLog {
	transferLogs := make([]state.TransferLog, len(curr))
	for i := 0; i < len(curr); i++ {
		transferLogs[i].Status = curr[i].Status
		transferLogs[i].LengthCmds = curr[i].LengthCmds
		transferLogs[i].Cmds = curr[i].Cmds
		transferLogs[i].Value = curr[i].Value
	}
	return transferLogs
}

func (r *Replica) handlePreAcceptOK(pareply *yatchproto.PreAcceptOK) {
	dlog.Println("Handling PreAcceptOK")
	//dlog.Println("pareply.LeaderId", pareply.LeaderId)
	//dlog.Println("pareply.Replica", pareply.Replica)
	//dlog.Println("pareply.LenCurrent", pareply.LenCurrent)
	//dlog.Println("pareply.LenPrevious", pareply.LenPrevious)
	//dlog.Println("pareply.Current", pareply.Current)
	//dlog.Println("pareply.Previous", pareply.Previous)
	//dlog.Println("pareply.Value", pareply.Value)
	//dlog.Println("pareply.CommitIndex", pareply.CommitIndex)

	//dlog.Println(,r.logs[pareply.Value])

	r.logs[pareply.Value].lb.preAcceptOKs++

	if r.logs[pareply.Value].lb.preAcceptOKs == r.N/2 {
		//if !allCommitted {
		//	weird++
		//}
		//slow++
		rr := pareply.Value/int32(r.N) - 1
		r.incrementCommitIndex(rr)
		//end := time.Since(start)
		//dlog.Println("time taken in handlePreAcceptOK", end)
		round := pareply.Value / int32(r.N)
		currRoundStart := round * int32(r.N)
		currRoundEnd := round*int32(r.N) + int32(r.N)
		currLogs := r.getLogsOfRound(currRoundStart, currRoundEnd)
		currTransferLogs := getLogsToTransferLogs(currLogs)
		previousRound := round - 1
		previousRoundStart := previousRound * int32(r.N)
		previousRoundEnd := previousRoundStart + int32(r.N)
		previousRoundLogs := r.getLogsOfRound(previousRoundStart, previousRoundEnd)
		prevTransferLogs := getLogsToTransferLogs(previousRoundLogs)
		if true {
			r.bcastAccept(r.Id, currTransferLogs, prevTransferLogs, pareply.Value, r.CommittedIndex)
		}
	}
	//TODO: take the slow path if messages are slow to arrive
}

func (r *Replica) handlePreAcceptNOK(pareply *yatchproto.PreAcceptNOK) {
	dlog.Println("Handling PreAcceptNOK")
	//dlog.Println("pareply.LeaderId", pareply.LeaderId)
	//dlog.Println("pareply.Replica", pareply.Replica)
	//dlog.Println("pareply.LenCurrent", pareply.LenCurrent)
	//dlog.Println("pareply.LenPrevious", pareply.LenPrevious)
	//dlog.Println("pareply.Current", pareply.Current)
	//dlog.Println("pareply.Previous", pareply.Previous)
	//dlog.Println("pareply.Value", pareply.Value)
	//dlog.Println("pareply.CommitIndex", pareply.CommitIndex)
	if r.CommittedIndex > pareply.CommitIndex {
		replicaCommitIndex := pareply.CommitIndex
		replicaCommitedRound := replicaCommitIndex / int32(r.N)
		prevStartIndex := replicaCommitedRound * int32(r.N)
		prevEndIndex := (r.currRound-1)*int32(r.N) + int32(r.N)
		prevLogs := r.getTransferLogs(prevStartIndex, prevEndIndex)
		currStartIndex := r.currRound * int32(r.N)
		currEndIndex := r.currRound*int32(r.N) + int32(r.N)
		currLogs := r.getTransferLogs(currStartIndex, currEndIndex)
		dlog.Println("send preAccept Again", pareply.Value, "to", pareply.Replica)
		r.sendPreAccept(pareply.Replica, pareply.Value, currLogs, prevLogs, r.CommittedIndex)
	} else {
		dlog.Println("in else case handlePreAcceptNOK")
	}

	//r.sendPreAccept(r.Id, pareply.Value, currTransferLogs, prevTransferLogs, r.CommittedIndex)
	//TODO: take the slow path if messages are slow to arrive
}

func (r *Replica) getTransferLogs(start int32, end int32) []state.TransferLog {
	curr := make([]state.TransferLog, end-start)
	for i := start; i < end; i++ {
		tlog := state.TransferLog{}
		if r.logs[i] == nil {
			tlog.Status = yatchproto.NONE
			tlog.LengthCmds = 0
			cmdss := []state.Command{}
			tlog.Cmds = cmdss
			tlog.Value = 0
		} else {
			tlog.Status = r.logs[i].Status
			tlog.LengthCmds = r.logs[i].LengthCmds
			tlog.Cmds = r.logs[i].Cmds
			tlog.Value = r.logs[i].Value
		}
		curr[i-start] = tlog
	}
	return curr
}

func (r *Replica) getLogsOfRound(start int32, end int32) []Log {
	curr := make([]Log, end-start)
	for i := start; i < end; i++ {
		log := Log{}
		if r.logs[i] == nil {
			log.Status = yatchproto.NONE
			log.LengthCmds = 0
			cmdss := []state.Command{}
			log.Cmds = cmdss
			log.Value = 0
		} else {
			log.Status = r.logs[i].Status
			log.LengthCmds = r.logs[i].LengthCmds
			log.Cmds = r.logs[i].Cmds
			log.Value = r.logs[i].Value
		}
		curr[i-start] = log
	}
	return curr
}

func previousRoundLogsSame(prev []Log, previous []state.TransferLog) bool {
	//dlog.Println("prev thisnode", prev)
	//dlog.Println("prev othernode", previous)
	if len(previous) != len(prev) {
		return false
	}
	for i := 0; i < len(prev); i++ {
		dlog.Println("previous at current node", previous[i], " i ", i)
		dlog.Println("previous at other node", prev[i], " i ", i)
		if prev[i].LengthCmds == 0 && previous[i].LengthCmds == 0 {
			continue
		} else if prev[i].LengthCmds == 0 || previous[i].LengthCmds == 0 {
			return false
		}
		if prev[i].Value != previous[i].Value {
			return false
		}
	}
	return true
}

var pa yatchproto.PreAccept
var pa2 yatchproto.PreAccept

func (r *Replica) bcastPreAccept(replica int32, value int32, currentRoundOfLogs []state.TransferLog, previousRoundOfLogs []state.TransferLog, commitIndex int32) {
	dlog.Println("bcastPreAccept", "replica", replica)
	defer func() {
		if err := recover(); err != nil {
			dlog.Println("PreAccept bcast failed:", err)
		}
	}()
	pa.LeaderId = r.Id
	//dlog.Println("preAccept.LeaderId", pa.LeaderId)
	pa.Replica = replica
	//dlog.Println("preAccept.Replica", pa.Replica)
	pa.Value = value
	//dlog.Println("preAccept.Value", pa.Value)
	pa.LenCurrent = int32(len(currentRoundOfLogs))
	//dlog.Println("preAccept.LenCurrent", pa.LenCurrent)
	pa.LenPrevious = int32(len(previousRoundOfLogs))
	//dlog.Println("preAccept.LenPrevious", pa.LenPrevious)
	pa.Current = currentRoundOfLogs

	//dlog.Println("preAccept.Current", pa.Current)
	pa.Previous = previousRoundOfLogs
	//dlog.Println("preAccept.Previous", pa.Previous)
	pa.CommitIndex = commitIndex
	//dlog.Println("preAccept.CommittedIndex", pa.CommitIndex)
	args := &pa

	n := r.N - 1
	if r.Thrifty {
		n = r.N / 2
	}

	sent := 0
	for q := 0; q < r.N; q++ {
		if !r.Alive[r.PreferredPeerOrder[q]] {
			continue
		}
		if int32(q) == r.Id {
			continue
		}
		r.SendMsg(r.PreferredPeerOrder[q], r.preAcceptRPC, args)
		sent++
		if sent >= n {
			break
		}
	}
}

func (r *Replica) sendPreAccept(replica int32, value int32, currentRoundOfLogs []state.TransferLog, previousRoundOfLogs []state.TransferLog, commitIndex int32) {
	dlog.Println("sendPreAccept", "replica", replica)
	defer func() {
		if err := recover(); err != nil {
			dlog.Println("PreAccept send failed:", err)
		}
	}()
	pa2.LeaderId = r.Id
	//dlog.Println("preAccept.LeaderId", pa.LeaderId)
	pa2.Replica = replica
	//dlog.Println("preAccept.Replica", pa.Replica)
	pa2.Value = value
	//dlog.Println("preAccept.Value", pa.Value)
	pa2.LenCurrent = int32(len(currentRoundOfLogs))
	//dlog.Println("preAccept.LenCurrent", pa.LenCurrent)
	pa2.LenPrevious = int32(len(previousRoundOfLogs))
	//dlog.Println("preAccept.LenPrevious", pa.LenPrevious)
	pa2.Current = currentRoundOfLogs

	//dlog.Println("preAccept.Current", pa.Current)
	pa2.Previous = previousRoundOfLogs
	//dlog.Println("preAccept.Previous", pa.Previous)
	pa2.CommitIndex = commitIndex
	//dlog.Println("preAccept.CommittedIndex", pa.CommitIndex)
	args := &pa2

	r.SendMsg(replica, r.preAcceptRPC, args)

}

var ea yatchproto.Accept

func (r *Replica) bcastAccept(replica int32, curr []state.TransferLog, prev []state.TransferLog, value int32, commitIndex int32) {
	dlog.Println("bcastAccept", "replica", replica, "value", value, "commitIndex", commitIndex)
	defer func() {
		if err := recover(); err != nil {
			dlog.Println("Accept bcast failed:", err)
		}
	}()

	ea.LeaderId = r.Id
	ea.Replica = replica
	ea.Value = value
	ea.CommitIndex = commitIndex
	args := &ea

	n := r.N - 1
	if r.Thrifty {
		n = r.N / 2
	}

	sent := 0
	for q := 0; q < r.N-1; q++ {
		if !r.Alive[r.PreferredPeerOrder[q]] {
			continue
		}
		if r.Id == int32(q) {
			continue
		}
		r.SendMsg(r.PreferredPeerOrder[q], r.acceptRPC, args)
		sent++
		if sent >= n {
			break
		}
	}
}

/**********************************************************************

                        PHASE 2

***********************************************************************/

func (r *Replica) handleAccept(accept *yatchproto.Accept) {
	rm := RecievedMessage{nil, accept, false}
	r.printMessageArgs(rm)
	r.getCurrentRoundAccept(rm)
}

func (r *Replica) handleAcceptReply(areply *yatchproto.AcceptReply) {
	start := time.Now()
	dlog.Println("handleAcceptReply")
	dlog.Println("AcceptReply.LeaderId", areply.LeaderId)
	dlog.Println("AcceptReply.Replica", areply.Replica)
	//dlog.Println("AcceptReply.LenCurrent", areply.LenCurrent)
	//dlog.Println("AcceptReply.LenPrevious", areply.LenPrevious)
	//dlog.Println("AcceptReply.Current", areply.Current)
	//dlog.Println("AcceptReply.Previous", areply.Previous)
	dlog.Println("AcceptReply.Value", areply.Value)
	dlog.Println("AcceptReply.CommitIndex", areply.CommitIndex)
	dlog.Println("r.logs[AcceptReply.Value]", r.logs[areply.Value])
	r.logs[areply.Value].lb.acceptOKs++

	if r.logs[areply.Value].lb.acceptOKs == r.N/2 {
		//r.InstanceSpace[areply.Replica][areply.Instance].Status = epaxosproto.COMMITTED
		//r.updateCommitted(areply.Replica)
		if r.logs[areply.Value].lb.clientProposals != nil && !r.Dreply {
			// give clients the all clear
			dlog.Println("give clients the all clear")
			dlog.Println("number of client proposals", len(r.logs[areply.Value].lb.clientProposals))
			for i := 0; i < len(r.logs[areply.Value].lb.clientProposals); i++ {
				r.ReplyProposeTS(
					&genericsmrproto.ProposeReplyTS{
						TRUE,
						r.logs[areply.Value].lb.clientProposals[i].CommandId,
						state.NIL,
						r.logs[areply.Value].lb.clientProposals[i].Timestamp},
					r.logs[areply.Value].lb.clientProposals[i].Reply)
			}
		}
		end := time.Since(start)
		dlog.Println("time taken handleAcceptReply", end)
	}
}

func (r *Replica) replyAccept(replicaId int32, reply *yatchproto.AcceptReply) {
	r.SendMsg(replicaId, r.acceptReplyRPC, reply)
}

func (r *Replica) unionOperationOnCurrentRound(round int32, transferLogs []state.TransferLog, currentLogAtNode []Log) {
	dlog.Println("unionOperationOnCurrentRound", round)
	dlog.Println("transferLogs", transferLogs)
	dlog.Println("currentLogAtNode", currentLogAtNode)
	start := round * int32(r.N)
	end := round*int32(r.N) + int32(r.N)

	for i := start; i < end; i++ {
		dlog.Println("(r.logs[i] == nil || r.logs[i].LengthCmds == 0)", (r.logs[i] == nil || r.logs[i].LengthCmds == 0))
		if (r.logs[i] == nil || r.logs[i].LengthCmds == 0) && transferLogs[i-start].LengthCmds > 0 {
			l := &Log{
				Cmds:       transferLogs[i-start].Cmds,
				Status:     transferLogs[i-start].Status,
				LengthCmds: transferLogs[i-start].LengthCmds,
				Value:      transferLogs[i-start].Value,
			}
			r.logs[i] = l
			dlog.Println("l.Value", l.Value)
			r.maxCommandValue = int32(math.Max(float64(r.maxCommandValue), float64(l.Value)))
			r.currRound = r.maxCommandValue / int32(r.N)
			dlog.Println("r.currRound", r.currRound)
		}
	}
}

func (r *Replica) printCurrentState() {
	dlog.Println("Replica", r.Id, "currentState")
	for i := 0; i < int(r.maxCommandValue); i++ {
		dlog.Println(r.logs[i])
	}
	dlog.Println(r.CommittedIndex)
	dlog.Println()
}
