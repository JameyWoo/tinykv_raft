package main

/*
每个raft都有一个rpc列表
通过这样调用: rf.peers[server].Call("Raft.RequestAppendEntries", req, resp)

初始化时要做的事:
1. 导入配置, 所有结点的rpc, 及结点的数量
2. 启动当前raft的rpc的listen, Dail其他的peer
*/

import (
	"fmt"
	"github.com/JameyWoo/tinykv_raft/raft"
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

func init() {
	//logrus.SetReportCaller(true) //垃圾垃圾垃圾垃圾垃圾输出
}

func ReadYamlConfig(path string) (*Config, error) {
	conf := &Config{}
	if f, err := os.Open(path); err != nil {
		return nil, err
	} else {
		_ = yaml.NewDecoder(f).Decode(conf)
	}
	return conf, nil
}

//************************************配置文件
type Config struct {
	Me    string   `yaml:"me"`
	Peers []string `yaml:"peers"`
}

type MyFormatter struct{}

func (s *MyFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	timestamp := time.Now().Local().Format("2006/01/02 15:04:05")
	fmt.Println(entry.Caller.File)
	fullPath := strings.Split(entry.Caller.File, "/")
	path := fullPath[len(fullPath) - 1] + ":" + strconv.Itoa(entry.Caller.Line)
	msg := fmt.Sprintf("%s %8s %15s\t %s\n",
		timestamp,
		"[" + strings.ToUpper(entry.Level.String()) + "]",
		path,
		entry.Message)
	return []byte(msg), nil
}

func init() {
	logrus.SetFormatter(new(MyFormatter))
	logrus.SetReportCaller(true)
}

func main() {
	fmt.Printf("E:/virtualShare/gopath3/src/tinykv_raft/tinykv.go \n")
	var configPath string
	if len(os.Args) > 1 {
		configPath = os.Args[1]
		logrus.Infof("config path: %s", configPath)
	} else {
		logrus.Panic("no configPath!")
	}

	conf, err := ReadYamlConfig(configPath)
	if err != nil {
		fmt.Println(err)
	}

	me := conf.Me
	peersHost := conf.Peers
	fmt.Println(me)
	fmt.Println(peersHost)

	addy, err := net.ResolveTCPAddr("tcp", me)
	if err != nil {
		panic(err)
	}

	// 先监听, 再连接
	listener, err := net.ListenTCP("tcp", addy)
	if err != nil {
		panic(err)
	}

	go func() {
		for {
			conn, _ := listener.Accept()
			logrus.Infof("receive from %s", conn.RemoteAddr())
			go rpc.ServeConn(conn)
		}
	}()

	// 所有的客户端
	var peers []*rpc.Client
	peers = make([]*rpc.Client,len(peersHost))

	indexMe := -1
	for k,v := range peersHost{
		if v == me {
			indexMe =k
			break
		}
	}
	if indexMe == -1 {
		panic("conf error")
	}

	waitGroup := sync.WaitGroup{}
	waitGroup.Add(len(peersHost) - 1)
	go func() {
		for i, peer := range peersHost {
			if i == indexMe {
				continue
			}
			peer := peer //一定要创建中间变量
			i := i
			go func() {
				for {
					client, err := rpc.Dial("tcp", peer)
					if err != nil {
						logrus.Warning(err)
						time.Sleep(3 * time.Second)
					} else {
						peers[i] = client
						waitGroup.Done()
						break
					}
				}
			}()
		}
	}()
	waitGroup.Wait() //等待所有初始化连接，之后挂了没关系， 但一定要有第一次

	logrus.Println("fuck")
	persister := raft.MakePersister()

	meRaft := raft.Make(peers, indexMe, persister)
	_ = rpc.Register(meRaft)
	logrus.Infof("start to Election")
	meRaft.ElectionLoop()

}
