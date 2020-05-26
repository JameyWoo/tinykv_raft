package raftrpc

import "net/rpc"

type ClientEnd struct {
	*rpc.Client
}

