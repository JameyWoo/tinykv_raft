package main

import (
	"fmt"
	"gopkg.in/yaml.v2"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"time"
)

// 模拟一个raft********************************************************
type Raft struct {
	Name  string        // 自己的名字，以端口号区分
	peers []*rpc.Client // 这里直接使用 rpc 的 client
}

// 模拟raft 的方法， 原来的 labrpc 是创建所有的方法
func (test *Raft) Handler1(args string, reply *int) error {
	args += " deal with handler1"
	*reply, _ = strconv.Atoi(args)
	return nil
}

func (test *Raft) Handler2(args int, reply *string) error {
	*reply = strconv.Itoa(args) + "deal with handler2"
	return nil
}

//read yaml config
//注：path为yaml或yml文件的路径
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

func main() {
	conf, err := ReadYamlConfig("D:\\go\\myproject\\tinykv_raft\\gorpc\\config.yaml")
	if err != nil {
		fmt.Println(err)
	}

	me := conf.Me
	peers := conf.Peers
	fmt.Println("****************************************")

	testRaft := new(Raft) // 创建 raft

	gorpc := new(gorpc)
	gorpc.Addr = me      //设置自己的host
	gorpc.Peers = peers  //设置peers 的服务
	gorpc.init(testRaft) // 初始化 raft

	for {
		time.Sleep(5 * time.Second)
	}
}

// 封装 go 的rpc
type gorpc struct {
	Addr  string   // 那个端口提供的服务， 在同一台机子上跑的话用端口作为区分
	Peers []string // 暂存
}

func (gorpc *gorpc) init(raft *Raft) {
	_ = rpc.Register(raft) //代理 raft 的所有服务
	tcpAddr, err := net.ResolveTCPAddr("tcp", gorpc.Addr)
	checkError(err)
	listener, err := net.ListenTCP("tcp", tcpAddr)
	checkError(err)
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				continue
			}
			go func() { // 每来一个 rpc ， 在这里记录日志
				fmt.Printf("receive from %s\n", conn.RemoteAddr())
				rpc.ServeConn(conn)
			}()
		}
	}()
	fmt.Println("init me over")
	for _, p := range gorpc.Peers {
		p := p // 要一个中间变量， c++ 的lambda 也要
		go func() { // 创建协程一直尝试去连接 peers
			for {
				client, err := rpc.Dial("tcp", p) // 这里会不会阻塞呢？？？？
				if err != nil {
					fmt.Println("Fatal error ", err.Error())
					time.Sleep(3 * time.Second) // 休息一下继续来
					continue
				}
				fmt.Printf("connect %s success", p)
				raft.peers = append(raft.peers, client)
				break
			}
		}()
	}
}

func checkError(err error) {
	if err != nil {
		fmt.Println("Fatal error ", err.Error())
		os.Exit(1)
	}
}
