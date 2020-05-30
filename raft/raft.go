package raft

import (
	"log"
	"net/rpc"
	"sync"
)
import "time"
import "math/rand"

//import "os"
//节点状态
const Follower, Leader, Candidate int = 1, 2, 3

//心跳周期
const HeartbeatDuration = time.Millisecond * 5000

//竞选周期
const CandidateDuration = HeartbeatDuration * 2

var raftOnce sync.Once

//状态机apply
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//日志
type LogEntry struct {
	Term  int
	Index int
	Log   interface{}
}

//日志快照
type LogSnapshot struct {
	Term  int
	Index int
	// 从偏移量开始的快照分块的原始字节(论文)
	Datas []byte
}

//投票请求
// TODO: 选举的Term和日志的Term什么区别? 日志的Term和Index又是什么区别?
type RequestVoteArgs struct {
	Me           int
	ElectionTerm int
	LogIndex     int
	LogTerm      int
}

//投票rpc返回
type RequestVoteReply struct {
	IsAgree     bool
	CurrentTerm int
}

//日志复制请求
type AppendEntries struct {
	Me           int
	Term         int
	PrevLogTerm  int
	PrevLogIndex int
	Entries      []LogEntry
	LeaderCommit int
	Snapshot     LogSnapshot //快照
}

//回复日志更新请求
type RespEntries struct {
	Term        int
	Successed   bool
	LastApplied int
}

type Raft struct {
	mu        sync.Mutex    // Lock to protect shared access to this peer's state
	peers     []*rpc.Client // rpc节点
	persister *Persister    // Object to hold this peer's persisted state
	me        int           // 自己服务编号

	logs            []LogEntry    // 日志存储
	logSnapshot     LogSnapshot   //日志快照
	commitIndex     int           //当前日志提交处
	lastApplied     int           //当前状态机执行处
	status          int           //节点状态
	currentTerm     int           //当前任期
	heartbeatTimers []*time.Timer //心跳定时器
	// 看用法: https://gobyexample.com/timers. 就是定时, 协程阻塞到 定时器的 .C上, 时间到了苏醒
	eletionTimer *time.Timer //竞选超时定时器
	randtime     *rand.Rand  //随机数，用于随机竞选周期，避免节点间竞争。

	nextIndex  []int //记录每个follow的同步日志状态
	matchIndex []int //记录每个follow日志最大索引，0递增
	isKilled bool          //节点退出
	lastLogs AppendEntries //最后更新日志
	LastGetLock string
}


func Make(peers []*rpc.Client, me int,
	persister *Persister) *Raft {
	rf := &Raft{}
	// 用 *rpc.Client 代表一个客户端(其他结点), 因为我只需要跟他们进行通信(rpc)
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	//rf.applyCh = applyCh
	rf.randtime = rand.New(rand.NewSource(time.Now().UnixNano() + int64(rf.me)))
	rf.isKilled = false
	rf.heartbeatTimers = make([]*time.Timer, len(rf.peers))
	rf.eletionTimer = time.NewTimer(CandidateDuration)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.setStatus(Follower)
	rf.lastLogs = AppendEntries{
		Me:   -1,
		Term: -1,
	}
	rf.logSnapshot = LogSnapshot{
		Index: 0,
		Term:  0,
	}
	//日志同步协程
	for i := 0; i < len(rf.peers); i++ {
		rf.heartbeatTimers[i] = time.NewTimer(HeartbeatDuration)
		go rf.ReplicateLogLoop(i)
	}
	rf.readPersist(persister.ReadRaftState())
	rf.apply()
	raftOnce.Do(func() {
		//filename :=  "log"+time.Now().Format("2006-01-02 15_04_05") +".txt"
		//file, _ := os.OpenFile(filename, os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
		//log.SetOutput(file)
		log.SetFlags(log.Ltime | log.Lmicroseconds)
	})
	//Leader选举协程
	//go rf.ElectionLoop()
	//放到主线程执行
	return rf
}