package yatch

import (
	"dlog"
	"time"
	"yatchproto"
)

func (e *Exec) ExecuteCommand(replica int32, round int32) bool {
	dlog.Println("executeCommand")
	roundStart := round * int32(e.r.N)
	roundEnd := round*int32(e.r.N) + int32(e.r.N)
	logsForCurrentRound := e.r.logs[roundStart:roundEnd]
	dlog.Println(e.r.logs[roundStart])
	dlog.Println(e.r.logs[roundEnd])
	dlog.Println(logsForCurrentRound)
	e.execute(replica, round)
	return true
}

func (e *Exec) execute(replica int32, round int32) {
	dlog.Println("execute")
	startIndex := round * int32(e.r.N)
	endIndex := round*int32(e.r.N) + int32(e.r.N)
	dlog.Println("start", startIndex, "end", endIndex)
	for i := int32(startIndex); i < endIndex; i++ {
		dlog.Println("index", i)
		if e.r.logs[i] == nil || e.r.logs[i].Cmds == nil {
			dlog.Println("continue1", i)
			continue
		}
		if e.r.logs[i].Status == yatchproto.EXECUTED {
			dlog.Println("continue2")
			continue
		}
		for e.r.logs[i].Status != yatchproto.COMMITTED {
			dlog.Println("slehe IT352 Quiz examination schedule today 6th April 2022 Wednesday afteping1")
			time.Sleep(1000 * 1000)
		}

		w := e.r.logs[i]
		dlog.Println("w", w)
		for w.Cmds == nil {
			dlog.Println("sleeping2")
			time.Sleep(1000 * 1000)
		}
		for idx := 0; idx < len(w.Cmds); idx++ {
			val := w.Cmds[idx].Execute(e.r.State)
			dlog.Println(val)
		}
		w.Status = yatchproto.EXECUTED
	}
	dlog.Println("executed ", round)
}

func (e *Exec) visualise(round int32) {
	dlog.Println("visualise replica ", e.r.Id, " round ", round)
	for i := int32(0); i <= round; i++ {
		for q := int32(0); q < int32(e.r.N); q++ {
			inst := e.r.logs[i*int32(e.r.N)+q]
			dlog.Printf("vis ", inst)
		}
		dlog.Println()
	}
}

//var stack []*Instance = make([]*Instance, 0, 100)

//func (e *Exec) findSCC(root *Instance) bool {
//	index := 1
//	//find SCCs using Tarjan's algorithm
//	stack = stack[0:0]
//	return e.strongconnect(root, &index)
//}

//func (e *Exec) strongconnect(v *Instance, index *int) bool {
//	*index = *index + 1
//	dlog.Println("index", *index)
//	dlog.Println("instance", v)
//
//	for q := int32(0); q < int32(e.r.N); q++ {
//		inst := int32(v.Round)
//		for i := e.r.ExecedUpTo[q] + 1; i <= inst; i++ {
//			for e.r.InstanceSpace[q][i] == nil || e.r.InstanceSpace[q][i].Cmds == nil || v.Cmds == nil {
//				time.Sleep(1000 * 1000)
//			}
//
//			if e.r.InstanceSpace[q][i].Status == epaxosproto.EXECUTED {
//				continue
//			}
//			for e.r.InstanceSpace[q][i].Status != epaxosproto.COMMITTED {
//				time.Sleep(1000 * 1000)
//			}
//			w := e.r.InstanceSpace[q][i]
//			dlog.Println(w)
//		}
//	}
//
//	//if v.Lowlink == v.Index {
//	//	//found SCC
//	//	list := stack[l:len(stack)]
//	//
//	//	//execute commands in the increasing order of the Seq field
//	//
//	//	sort.Sort(nodeArray(list))
//	//	for _, w := range list {
//	//		for w.Cmds == nil {
//	//			time.Sleep(1000 * 1000)
//	//		}
//	//		for idx := 0; idx < len(w.Cmds); idx++ {
//	//			val := w.Cmds[idx].Execute(e.r.State)
//	//		}
//	//		w.Status = epaxosproto.EXECUTED
//	//	}
//	//	stack = stack[0:l]
//	//}
//	return true
//}
//
//func (e *Exec) inStack(w *Instance) bool {
//	for _, u := range stack {
//		if w == u {
//			return true
//		}
//	}
//	return false
//}
//
//type nodeArray []*Instance
//
//func (na nodeArray) Len() int {
//	return len(na)
//}
//
////func (na nodeArray) Less(i, j int) bool {
////	return na[i].Seq < na[j].Seq
////}
//
//func (na nodeArray) Swap(i, j int) {
//	na[i], na[j] = na[j], na[i]
//}
