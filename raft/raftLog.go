package raft

import (
	"bytes"
	"github.com/JameyWoo/tinykv_raft/labgob"
	"github.com/sirupsen/logrus"
	"sort"
	"time"
)

//同步日志RPC
func (rf *Raft) sendAppendEnteries(server int, req *AppendEntries, resp *RespEntries) bool {
	rstChan := make(chan (bool))
	ok := false
	go func() {
		rst := rf.peers[server].Call("Raft.RequestAppendEntries", req, resp)
		ok := true
		if rst != nil {
			ok = false
		}
		rstChan <- ok
	}()
	select {
	case ok = <-rstChan:
	case <-time.After(time.Millisecond * 3000):
	}
	return ok
}

//更新插入同步日志
func (rf *Raft) updateLog(start int, logEntrys []LogEntry, snapshot *LogSnapshot) {
	rf.lock("Raft.updateLog")
	defer rf.unlock("Raft.updateLog")
	if snapshot.Index > 0 { //更新快照
		rf.logSnapshot = *snapshot
		start = rf.logSnapshot.Index
		rf.println("update snapshot :", rf.me, rf.logSnapshot.Index, "len logs", len(logEntrys))
	}
	index := start - rf.logSnapshot.Index
	for i := 0; i < len(logEntrys); i++ {
		if index+i < 0 {
			//网络不可靠，follower节点成功apply并保存快照后，Leader未收到反馈，重复发送日志，
			//可能会导致index <0情况。
			continue
		}
		if index+i < len(rf.logs) {
			rf.logs[index+i] = logEntrys[i]
		} else {
			rf.logs = append(rf.logs, logEntrys[i])
		}
	}
	size := index + len(logEntrys)
	if size < 0 { //网络不可靠+各节点独立备份快照可能出现
		size = 0
	}
	//重置log大小
	rf.logs = rf.logs[:size]
}

//插入日志
func (rf *Raft) insertLog(command interface{}) int {
	rf.lock("Raft.insertLog")
	defer rf.unlock("Raft.insertLog")
	entry := LogEntry{
		Term:  rf.currentTerm,
		Index: 1,
		Log:   command,
	}
	//获取log索引
	if len(rf.logs) > 0 {
		entry.Index = rf.logs[len(rf.logs)-1].Index + 1
	} else {
		entry.Index = rf.logSnapshot.Index + 1
	}
	//插入log
	rf.logs = append(rf.logs, entry)
	return entry.Index
}

//获取当前已被提交日志
func (rf *Raft) updateCommitIndex() bool {
	rst := false
	var indexs []int
	rf.matchIndex[rf.me] = 0
	if len(rf.logs) > 0 {
		rf.matchIndex[rf.me] = rf.logs[len(rf.logs)-1].Index
	} else {
		rf.matchIndex[rf.me] = rf.logSnapshot.Index
	}
	for i := 0; i < len(rf.matchIndex); i++ {
		indexs = append(indexs, rf.matchIndex[i])
	}
	sort.Ints(indexs)
	index := len(indexs) / 2
	commit := indexs[index]
	if commit > rf.commitIndex {
		rf.println(rf.me, "update leader commit index", commit)
		rst = true
		rf.commitIndex = commit
	}
	return rst
}

//apply 状态机， 改变， 当前的raft commit 节点
func (rf *Raft) apply() {
	rf.lock("Raft.apply")
	defer rf.unlock("Raft.apply")
	if rf.status == Leader {
		rf.updateCommitIndex()
	}

	lastapplied := rf.lastApplied
	if rf.lastApplied < rf.logSnapshot.Index {
		msg := ApplyMsg{
			CommandValid: false,
			Command:      rf.logSnapshot,
			CommandIndex: 0,
		}
		logrus.Print("apply msg: ", msg)
		//rf.applyCh <- msg
		rf.lastApplied = rf.logSnapshot.Index
		rf.println(rf.me, "apply snapshot :", rf.logSnapshot.Index, "with logs:", len(rf.logs))
	}
	last := 0
	if len(rf.logs) > 0 {
		last = rf.logs[len(rf.logs)-1].Index
	}
	for ; rf.lastApplied < rf.commitIndex && rf.lastApplied < last; rf.lastApplied++ {
		index := rf.lastApplied
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[index-rf.logSnapshot.Index].Log,
			CommandIndex: rf.logs[index-rf.logSnapshot.Index].Index,
		}
		logrus.Print("apply msg: ", msg)
		//rf.applyCh <- msg
	}
	if rf.lastApplied > lastapplied {
		appliedIndex := rf.lastApplied - 1 - rf.logSnapshot.Index
		endIndex, endTerm := 0, 0
		if appliedIndex < 0 {
			endIndex = rf.logSnapshot.Index
			endTerm = rf.logSnapshot.Term
		} else {
			endTerm = rf.logs[rf.lastApplied-1-rf.logSnapshot.Index].Term
			endIndex = rf.logs[rf.lastApplied-1-rf.logSnapshot.Index].Index
		}
		rf.println(rf.me, "apply log", rf.lastApplied-1, endTerm, "-", endIndex, "/", last)
	}
}

//设置最后一次提交（用于乱序判定）
func (rf *Raft) setLastLog(req *AppendEntries) {
	rf.lock("Raft.setLastLog")
	defer rf.unlock("Raft.setLastLog")
	rf.lastLogs = *req
}

//判定乱序
func (rf *Raft) isOldRequest(req *AppendEntries) bool {
	rf.lock("Raft.isOldRequest")
	defer rf.unlock("Raft.isOldRequest")
	if req.Term == rf.lastLogs.Term && req.Me == rf.lastLogs.Me {
		lastIndex := rf.lastLogs.PrevLogIndex + rf.lastLogs.Snapshot.Index + len(rf.lastLogs.Entries)
		reqLastIndex := req.PrevLogIndex + req.Snapshot.Index + len(req.Entries)
		return lastIndex > reqLastIndex
	}
	return false
}

func (rf *Raft) persist() {
	rf.lock("Raft.persist")
	writer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(writer)
	encoder.Encode(rf.currentTerm)
	encoder.Encode(rf.commitIndex)
	encoder.Encode(rf.logs)
	encoder.Encode(rf.lastLogs)
	encoder.Encode(rf.logSnapshot.Index)
	encoder.Encode(rf.logSnapshot.Term)
	data := writer.Bytes()
	//rf.persister.SaveRaftState(data)
	rf.unlock("Raft.persist")
	rf.persister.SaveStateAndSnapshot(data, rf.logSnapshot.Datas)
	//持久化数据后，apply 通知触发快照
	msg := ApplyMsg{
		CommandValid: false,
		Command:      nil,
		CommandIndex: 0,
	}
	logrus.Print("apply msg: ", msg)
	//rf.applyCh <- msg
}

func (rf *Raft) readPersist(data []byte) {
	rf.lock("Raft.readPersist")
	if data == nil || len(data) < 1 {
		rf.unlock("Raft.readPersist")
		return
	}
	reader := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(reader)
	var commitIndex, currentTerm int
	var logs []LogEntry
	var lastlogs AppendEntries
	if decoder.Decode(&currentTerm) != nil ||
		decoder.Decode(&commitIndex) != nil ||
		decoder.Decode(&logs) != nil ||
		decoder.Decode(&lastlogs) != nil ||
		decoder.Decode(&rf.logSnapshot.Index) != nil ||
		decoder.Decode(&rf.logSnapshot.Term) != nil {
		rf.println("Error in unmarshal raft state")
	} else {

		rf.currentTerm = currentTerm
		rf.commitIndex = commitIndex
		rf.lastApplied = 0
		rf.logs = logs
		rf.lastLogs = lastlogs
	}
	rf.unlock("Raft.readPersist")
	rf.logSnapshot.Datas = rf.persister.ReadSnapshot()
}

// 可以用来做心跳包
func (rf *Raft) RequestAppendEntries(req *AppendEntries, resp *RespEntries) error {
	logrus.Infof("RequestAppendEntries from %d, his term: %d", req.Me, req.Term)
	currentTerm, _ := rf.GetState()
	resp.Term = currentTerm
	resp.Successed = true
	if req.Term < currentTerm {
		// leader任期小于自身任期，则拒绝同步log
		resp.Successed = false
		return nil
	}
	//乱序日志，不处理
	if rf.isOldRequest(req) {
		return nil
	}
	// 否则更新自身任期，切换自生为follow，重置选举定时器
	// 每次收到消息之后都会重置, 所以这个时间超时了worker就会发起投票
	rf.resetCandidateTimer()//******************************************重置选举时间********************************************
	rf.setTerm(req.Term)
	rf.setStatus(Follower)
	_, logindex := rf.getLogTermAndIndex()
	//判定与leader日志是一致
	if req.PrevLogIndex > 0 {
		if req.PrevLogIndex > logindex {
			//没有该日志，则拒绝更新
			//rf.println(rf.me, "can't find preindex", req.PrevLogTerm)
			resp.Successed = false
			resp.LastApplied = rf.lastApplied
			return nil
		}
		if rf.getLogTermOfIndex(req.PrevLogIndex) != req.PrevLogTerm {
			//该索引与自身日志不同，则拒绝更新
			//rf.println(rf.me, "term error", req.PrevLogTerm)
			resp.Successed = false
			resp.LastApplied = rf.lastApplied
			return nil
		}
	}
	//更新日志
	rf.setLastLog(req)
	if len(req.Entries) > 0 || req.Snapshot.Index > 0 {
		if len(req.Entries) > 0 {
			logrus.Println(rf.me, "update log from ", req.Me, ":", req.Entries[0].Term, "-", req.Entries[0].Index, "to", req.Entries[len(req.Entries)-1].Term, "-", req.Entries[len(req.Entries)-1].Index)
		}
		rf.updateLog(req.PrevLogIndex, req.Entries, &req.Snapshot)
	}
	rf.setCommitIndex(req.LeaderCommit)
	rf.apply()
	rf.persist()

	return nil
}

//复制日志给follower
func (rf *Raft) replicateLogTo(peer int) bool {
	replicateRst := false
	if peer == rf.me {
		return replicateRst
	}
	isLoop := true
	for isLoop {
		time.Sleep(3 * time.Second) // 小于心跳， 一定要
		isLoop = false
		currentTerm, isLeader := rf.GetState()
		if !isLeader || rf.isKilled {
			break
		}
		// 获取当前peer的日志信息, 其中有 leader给这个追随者的index, 强制删除冲突的日志并加上领导人的日志
		req := rf.getAppendEntries(peer)
		resp := RespEntries{Term: 0}
		// RPC调用, 请求日志同步(复制)
		rst := rf.sendAppendEnteries(peer, &req, &resp)
		currentTerm, isLeader = rf.GetState()
		if rst && isLeader {
			// 如果某个节点任期大于自己，则更新任期，变成follow
			if resp.Term > currentTerm {
				logrus.Println(rf.me, "become follow ", peer, "term :", resp.Term)
				rf.setTerm(resp.Term)
				rf.setStatus(Follower)
			} else if !resp.Successed { //如果更新失败则更新follow日志next索引
				//rf.incNext(peer)
				rf.setNext(peer, resp.LastApplied+1)
				isLoop = true
			} else { //更新成功
				if len(req.Entries) > 0 {
					rf.setNextAndMatch(peer, req.Entries[len(req.Entries)-1].Index)
					replicateRst = true
				} else if req.Snapshot.Index > 0 {
					rf.setNextAndMatch(peer, req.Snapshot.Index)
					replicateRst = true
				}
			}
		} else {
			isLoop = true
		}
	}
	return replicateRst
}

//立即复制日志
func (rf *Raft) replicateLogNow() {
	rf.lock("Raft.replicateLogNow")
	defer rf.unlock("Raft.replicateLogNow")
	for i := 0; i < len(rf.peers); i++ {
		rf.heartbeatTimers[i].Reset(0)
	}
}

//心跳周期复制日志loop
func (rf *Raft) ReplicateLogLoop(peer int) {
	defer func() {
		rf.heartbeatTimers[peer].Stop()
	}()
	for !rf.isKilled {
		<-rf.heartbeatTimers[peer].C
		if rf.isKilled {
			break
		}
		rf.lock("Raft.ReplicateLogLoop")
		rf.heartbeatTimers[peer].Reset(HeartbeatDuration)
		rf.unlock("Raft.ReplicateLogLoop")
		_, isLeader := rf.GetState()
		//*********************************************************************************************************************
		if isLeader { // 函数是被协程调用的，一个 peer 一个协程， 如果自己是leader 才去调用，巧妙
			logrus.Println("leader ", rf.me, " ask ", peer, "to sync log")
			success := rf.replicateLogTo(peer)
			if success {
				rf.apply()
				rf.replicateLogNow()
				rf.persist()
			}
		}
	}
	logrus.Println(rf.me, "-", peer, "Exit ReplicateLogLoop")
}

func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	index = 0
	term, isLeader = rf.GetState()
	if isLeader {
		//设置term并插入log
		index = rf.insertLog(command)
		rf.println("leader", rf.me, ":", "append log", term, "-", index)
		rf.replicateLogNow()
	}
	return
}

func (rf *Raft) Kill() {
	rf.isKilled = true
	rf.eletionTimer.Reset(0)
	rf.replicateLogNow()
}

//保存快照
func (rf *Raft) SaveSnapshot(index int, snapshot []byte) {
	rf.lock("Raft.SaveSnapshot")
	if index > rf.logSnapshot.Index {
		//保存快照
		start := rf.logSnapshot.Index
		rf.logSnapshot.Index = index
		rf.logSnapshot.Datas = snapshot
		rf.logSnapshot.Term = rf.logs[index-start-1].Term
		//删除快照日志
		if len(rf.logs) > 0 {
			rf.logs = rf.logs[(index - start):]
		}
		rf.println("save snapshot :", rf.me, index, ",len logs:", len(rf.logs))
		rf.unlock("Raft.SaveSnapshot")
		rf.persist()
	} else {
		rf.unlock("Raft.SaveSnapshot")
	}

}
