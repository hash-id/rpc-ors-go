package main

import (
	"fmt"
	"runtime"

	"github.com/go-redis/redis/v8"
	"github.com/hash-id/rpc-ors-go/rpcors"
	"github.com/teambition/jsonrpc-go"
)

func main() {
	runtime.GOMAXPROCS(2) // set 1 for testing
	server := rpcors.NewService(rpcors.ServiceConfig{
		Name: "nodejs",
	}, redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	server.Register("method-sample", func(data *jsonrpc.RPC) ([]byte, *jsonrpc.ErrorObj) {
		fmt.Println("method-sample-called", data.ID)
		return jsonrpc.Success(1234, "demosaja")
		// return jsonrpc.Error(data.ID, jsonrpc.ErrorWith(20123, "failed"))
	})

	select {}
}
