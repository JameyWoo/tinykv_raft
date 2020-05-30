package raft

import "log"

func (rf *Raft) lock(info string) {
	rf.mu.Lock()
	rf.LastGetLock = info
}

func (rf *Raft) unlock(info string) {
	rf.LastGetLock = ""
	rf.mu.Unlock()
}

//打印调试日志
func (rf *Raft) println(args ...interface{}) {
	log.Println(args...)
}

//获取状态和任期
func (rf *Raft) GetState() (int, bool) {
	rf.lock("Raft.GetState")
	defer rf.unlock("Raft.GetState")
	isleader := rf.status == Leader
	return rf.currentTerm, isleader
}

//设置任期
func (rf *Raft) setTerm(term int) {
	rf.lock("Raft.setTerm")
	defer rf.unlock("Raft.setTerm")
	rf.currentTerm = term
}

//增加任期
func (rf *Raft) addTerm(term int) {
	rf.lock("Raft.addTerm")
	defer rf.unlock("Raft.addTerm")
	rf.currentTerm += term
}

//设置当前节点状态
func (rf *Raft) setStatus(status int) {
	rf.lock("Raft.setStatus")
	defer rf.unlock("Raft.setStatus")
	// 设置节点状态，变换为follow时候重置选举定时器（避免竞争）
	if (rf.status != Follower) && (status == Follower) {
		rf.resetCandidateTimer()//******************************************重置选举时间********************************************
	}

	// 节点变为leader，则初始化follow日志状态
	if rf.status != Leader && status == Leader {
		index := len(rf.logs)
		// TODO: 什么意思?
		// 应该是每次做 Snapshot 时候, 会把 rf.logs 清空, 所以计算nextIndex需要加上快照的index
		for i := 0; i < len(rf.peers); i++ {
			// TODO: nextIndex 在哪里用到了? 他好像没有用来设置同步啊
			rf.nextIndex[i] = index + 1 + rf.logSnapshot.Index
			rf.matchIndex[i] = 0
		}
	}
	rf.status = status
}

//获取状态
func (rf *Raft) getStatus() int {
	rf.lock("Raft.getStatus")
	defer rf.unlock("Raft.getStatus")
	return rf.status
}

//获取提交日志索引
func (rf *Raft) getCommitIndex() int {
	rf.lock("Raft.getCommitedCnt")
	defer rf.unlock("Raft.getCommitedCnt")
	return rf.commitIndex
}

//设置提交日志索引
func (rf *Raft) setCommitIndex(index int) {
	rf.lock("Raft.setCommitIndex")
	defer rf.unlock("Raft.setCommitIndex")
	rf.commitIndex = index
}

//获取日志索引及任期
func (rf *Raft) getLogTermAndIndex() (int, int) {
	rf.lock("Raft.getLogTermAndIndex")
	defer rf.unlock("Raft.getLogTermAndIndex")
	index := 0
	term := 0
	size := len(rf.logs)
	if size > 0 {
		index = rf.logs[size-1].Index
		term = rf.logs[size-1].Term
	} else {
		index = rf.logSnapshot.Index
		term = rf.logSnapshot.Term
	}
	return term, index
}

//获取索引处及任期
func (rf *Raft) getLogTermOfIndex(index int) int {
	rf.lock("Raft.getLogTermOfIndex")
	defer rf.unlock("Raft.getLogTermOfIndex")
	index -=(1+rf.logSnapshot.Index)
	if index < 0 {
		return rf.logSnapshot.Term
	}
	return rf.logs[index].Term
}

//获取快照
func (rf *Raft) getSnapshot(index int, snapshot *LogSnapshot) int {
	if index <= rf.logSnapshot.Index { //如果follow日志小于快照，则获取快照
		*snapshot = rf.logSnapshot
		index = 0 //更新快照时，从0开始复制日志
	} else {
		index -= rf.logSnapshot.Index
	}
	return index
}

//获取该节点更新日志
func (rf *Raft) getEntriesInfo(index int, snapshot *LogSnapshot,entries *[]LogEntry) (preterm int, preindex int) {
	start := rf.getSnapshot( index , snapshot)-1
	if start < 0 {
		preindex = 0
		preterm = 0
	} else if start == 0 {
		if rf.logSnapshot.Index ==0 {
			preindex = 0
			preterm = 0
		}  else {
			preindex = rf.logSnapshot.Index
			preterm = rf.logSnapshot.Term
		}
	} else {
		preindex = rf.logs[start-1].Index
		preterm = rf.logs[start-1].Term
	}
	if start < 0 {
		start = 0
	}
	for i := start; i < len(rf.logs); i++ {
		*entries = append(*entries, rf.logs[i])
	}
	return
}

//获取该节点更新日志及信息
func (rf *Raft) getAppendEntries(peer int) AppendEntries {
	rf.lock("Raft.getAppendEntries")
	defer rf.unlock("Raft.getAppendEntries")
	rst := AppendEntries{
		Me:           rf.me,
		Term:         rf.currentTerm,
		LeaderCommit: rf.commitIndex ,
		Snapshot : LogSnapshot{Index:0},
	}
	//当前follow的日志状态
	next := rf.nextIndex[peer]
	rst.PrevLogTerm, rst.PrevLogIndex = rf.getEntriesInfo(next, &rst.Snapshot, &rst.Entries)
	return rst
}

//减少follow next日志索引
func (rf *Raft) incNext(peer int) {
	rf.lock("Raft.incNext")
	defer rf.unlock("Raft.incNext")
	if rf.nextIndex[peer] > 1 {
		rf.nextIndex[peer]--
	}
}

//设置follow next日志索引
func (rf *Raft) setNext(peer int, next int) {
	rf.lock("Raft.setNext")
	defer rf.unlock("Raft.setNext")
	rf.nextIndex[peer] = next
}

// 设置follower next和match日志索引
func (rf *Raft) setNextAndMatch(peer int, index int) {
	rf.lock("Raft.setNextAndMatch")
	defer rf.unlock("Raft.setNextAndMatch")
	rf.nextIndex[peer] = index + 1
	rf.matchIndex[peer] = index
}

