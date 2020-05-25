package main

import (
	"fmt"
	"log"
	"net/rpc"
	"time"
)

type Args struct {
	A, B int
}

type Quotient struct {
	Quo, Rem int
}

func main() {
	//if len(os.Args) != 2 {
	//	fmt.Println("Usage: ", os.Args[0], "server:port")
	//	os.Exit(1)
	//}

	service := "127.0.0.1:1235"

	client, err := rpc.Dial("tcp", service)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	// Synchronous call
	args := Args{17, 8}
	var reply int

	rstChan := make(chan error)

	go func() {
		err = client.Call("Arith.Multiply", args, &reply)
		rstChan <- err
	}()

	select {
	case <-rstChan:
		fmt.Print(err)
	case <-time.After(6 * time.Second):
	}

	if err != nil {
		log.Fatal("arith error:", err)
	}
	fmt.Printf("Arith: %d*%d=%d\n", args.A, args.B, reply)

	var quot Quotient
	err = client.Call("Arith.Divide", args, &quot)
	if err != nil {
		log.Fatal("arith error:", err)
	}
	fmt.Printf("Arith: %d/%d=%d remainder %d\n", args.A, args.B, quot.Quo, quot.Rem)

}
