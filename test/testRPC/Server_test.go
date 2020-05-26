package main

import (
	"errors"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"testing"
	"time"
)

type Args struct {
	A, B int
}

type Quotient struct {
	Quo, Rem int
}

type Arith int

func (t *Arith) Multiply(args *Args, reply *int) bool {
	*reply = args.A * args.B
	return true
}

func (t *Arith) Divide(args *Args, quo *Quotient) error {
	if args.B == 0 {
		return errors.New("divide by zero")
	}
	quo.Quo = args.A / args.B
	quo.Rem = args.A % args.B
	return nil
}

func TestServer(t *testing.T) {

	arith := new(Arith)
	fmt.Println(arith)
	_ = rpc.Register(arith)

	tcpAddr, err := net.ResolveTCPAddr("tcp", ":1235")
	checkError(err)

	listener, err := net.ListenTCP("tcp", tcpAddr)
	checkError(err)

	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		fmt.Printf("receive from %s\n",conn.RemoteAddr() )
		time.Sleep(1 * time.Second)
		rpc.ServeConn(conn)
	}

}

func checkError(err error) {
	if err != nil {
		fmt.Println("Fatal error ", err.Error())
		os.Exit(1)
	}
}