package main

/*
每个raft都有一个rpc列表
通过这样调用: rf.peers[server].Call("Raft.RequestAppendEntries", req, resp)

初始化时要做的事:
1. 导入配置, 所有结点的rpc, 及结点的数量
2. 启动当前raft的rpc的listen, Dail其他的peer
 */

import (
	"github.com/JameyWoo/tinykv_raft/raft"
	"github.com/sirupsen/logrus"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"time"
)

var acceptOver chan int

func init() {
	//logrus.SetReportCaller(true)
}

func main() {
	// 所有的客户端
	var clients []*rpc.Client
	me, _ := strconv.Atoi(os.Args[1])
	me -= 1

	clientList := []string{"127.0.0.1:8001", "127.0.0.1:8002", "127.0.0.1:8003"}
	addy, err := net.ResolveTCPAddr("tcp", clientList[me])
	if err != nil {
		panic(err)
	}

	// 先监听, 再连接
	listener, err := net.ListenTCP("tcp", addy)
	if err != nil {
		panic(err)
	}

	time.Sleep(time.Second * 5)

	go func() {
		for {
			conn, _ := listener.Accept()
			logrus.Infof("receive from %s",conn.RemoteAddr())
			go rpc.ServeConn(conn)
		}
		acceptOver <- 1
	}()

	for i, cli := range clientList {
		if i == me {
			continue
		}
		client, err := rpc.Dial("tcp", cli)
		if err != nil {
			panic(err)
		}
		clients = append(clients, client)
	}
	applyCh := make(chan raft.ApplyMsg)
	persister := raft.MakePersister()

	meRaft := raft.Make(clients, me, persister, applyCh)
	rpc.Register(meRaft)
	logrus.Infof("start to Election")
	meRaft.ElectionLoop()

	<- acceptOver
}