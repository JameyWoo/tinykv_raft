package raft

import (
	"github.com/sirupsen/logrus"
	"sync"
	"time"
)

func (rf *Raft) Vote() {
	logrus.Infof("%d start to vote", rf.me)
	// 票先增大自身任期
	rf.addTerm(1)
	logterm, logindex := rf.getLogTermAndIndex()
	currentTerm, _ := rf.GetState()
	// 先构造请求投票参数
	req := RequestVoteArgs{
		Me:           rf.me,
		ElectionTerm: currentTerm,
		LogTerm:      logterm,
		LogIndex:     logindex,
	}
	var wait sync.WaitGroup
	peercnt := len(rf.peers)
	wait.Add(peercnt)
	agreeVote := 0
	term := currentTerm
	for i := 0; i < peercnt; i++ {
		//并行调用投票rpc，避免单点阻塞
		go func(index int) {
			defer wait.Done()
			resp := RequestVoteReply{false, -1}
			// 这个请求投票是自己, 直接同意然后返回
			if index == rf.me {
				agreeVote++
				return
			}
			// RPC 发送投票请求, 将结果保存到 resp
			logrus.Infof("me: %d ask %d to vote", rf.me, index)
			rst := rf.sendRequestVote(index, &req, &resp)
			// rpc调用失败
			if !rst {
				return
			}
			// 统计 同意的个数
			if resp.IsAgree {
				agreeVote++
				return
			}
			if resp.CurrentTerm > term {
				term = resp.CurrentTerm
			}

		}(i)
	}
	wait.Wait()
	logrus.Infof("me: %d get %d votes!", rf.me, agreeVote)
	//如果存在系统任期更大，则更新任期并转为follow
	if term > currentTerm {
		rf.setTerm(term)
		rf.setStatus(Follower)
	} else if agreeVote*2 > peercnt { // 获得多数赞同则变成leader
		logrus.Println("me:", rf.me, " become leader, currentTerm is ", currentTerm)
		rf.setStatus(Leader)
		rf.replicateLogNow()
	}
}

//投票RPC
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	rstChan := make(chan (bool))
	ok := false
	go func() {
		// RPC
		//logrus.Print(server, len(rf.peers))
		rst := rf.peers[server].Call("Raft.RequestVote", args, reply)
		ok := true
		if rst != nil {
			logrus.Println("me: ",rf.me, " rpc call", server, "failed")
			ok = false
		}
		logrus.Print("me: ",rf.me, " isAgree( ", reply.IsAgree, ") me: ", args.Me, " to be leader")
		rstChan <- ok
	}()
	select {
	case ok = <-rstChan:
	case <-time.After(HeartbeatDuration):
		//rpc调用超时
	}
	return ok
}

// 一个随机的定时, 重置竞选周期定时. 竞选周期!
func (rf *Raft) resetCandidateTimer() {
	randCnt := rf.randtime.Intn(250)
	duration := time.Duration(randCnt)*time.Millisecond + CandidateDuration
	rf.eletionTimer.Reset(duration)
}


// 收到投票请求
func (rf *Raft) RequestVote(req *RequestVoteArgs, reply *RequestVoteReply) error {
	logrus.Println(rf.me, " RequestVote ", req.Me, " to vote")
	reply.IsAgree = true
	reply.CurrentTerm, _ = rf.GetState()
	// 竞选任期小于自身任期，则反对票
	if reply.CurrentTerm >= req.ElectionTerm {
		logrus.Println("me:", rf.me, " refuse ", req.Me, "because of term")
		reply.IsAgree = false
		return nil
	}
	//竞选任期大于自身任期，则更新自身任期，并设置自己为follow
	rf.setStatus(Follower)
	rf.setTerm(req.ElectionTerm)
	logterm, logindex := rf.getLogTermAndIndex()
	// 判定竞选者日志是否新于自己, logterm 是自己的
	if logterm > req.LogTerm {
		// 日志比自己旧, 那么拒绝
		logrus.Println(rf.me, "refuse", req.Me, "because of logs's term")
		reply.IsAgree = false
	} else if logterm == req.LogTerm {
		reply.IsAgree = logindex <= req.LogIndex
		if !reply.IsAgree {
			logrus.Println(rf.me, "refuse", req.Me, "because of logs's index")
		}
	}
	if reply.IsAgree {
		logrus.Println(rf.me, "agree", req.Me)
		//赞同票后重置选举定时，避免竞争
		rf.resetCandidateTimer()
	}
	return nil
}


// TODO: so, 这个选举是怎么运行的. 不是说要么任期到期要么自己收不到消息(leader宕机)才发起选举吗
// 选举定时器loop
func (rf *Raft) ElectionLoop() {
	//选举超时定时器
	rf.resetCandidateTimer()
	defer rf.eletionTimer.Stop()

	// 只要结点一直在运行, 就一直执行状态机
	for !rf.isKilled {
		// TODO: 这个定时器是一个任期的时间嘛? 应该不是, 是竞选周期
		<-rf.eletionTimer.C
		if rf.isKilled {
			break
		}
		if rf.getStatus() == Candidate {
			//如果状态为竞选者，则直接发动投票
			rf.resetCandidateTimer()
			logrus.Infof("candidate me:%d start to call Vote()", rf.me)
			rf.Vote()
		} else if rf.getStatus() == Follower {
			//如果状态为follow，则转变为candidata并发动投票
			rf.setStatus(Candidate)
			logrus.Infof("follower me:%d start to be Cand and Vote()", rf.me)
			rf.resetCandidateTimer()
			rf.Vote()
		}
	}
	logrus.Println(rf.me, "Exit ElectionLoop")
}
