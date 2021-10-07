package main

import (
	"context"
	"log"
	"runtime"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/hash-id/rpc-ors-go/rpcors"
)

func main() {
	runtime.GOMAXPROCS(1) // set 1 for testing
	client := rpcors.NewClient(rpcors.ClientConfig{
		Name:        "kotlin-client",
		Destination: "nodejs",
	}, redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	ctx := context.Background()
	time.Sleep(3 * time.Second)

	totalReq := 5000
	wg := sync.WaitGroup{}
	wg.Add(totalReq)

	defer timeTrack(time.Now(), "client")

	for i := 0; i < totalReq; i++ {
		go func() {
			ctxTimeout, cancel := context.WithTimeout(ctx, time.Second*10)
			defer cancel()
			reply, err := client.Call(ctxTimeout, "method-sample", "nothing")
			if err != nil {
				log.Println(err)
			} else {
				log.Println("Receive reply")
				log.Println((*reply).Result, (*reply).Error)
			}
			wg.Done()
		}()
	}
	wg.Wait()

}

func timeTrack(start time.Time, name string) {
	elapsed := time.Since(start)
	log.Printf("%s took %s", name, elapsed)
}
