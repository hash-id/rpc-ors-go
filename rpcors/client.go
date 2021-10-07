package rpcors

import (
	"context"
	"errors"
	"fmt"
	"log"
	"runtime"
	"time"

	"github.com/alitto/pond"
	"github.com/go-redis/redis/v8"
	jsonrpc "github.com/teambition/jsonrpc-go"
)

type RpcClient struct {
	client       *redis.Client
	name         string
	dest         string
	lastReadId   string
	replyChMap   SafeChannelMap
	replayStream string
	pool         *pond.WorkerPool
}

type ClientConfig struct {
	// current service (self)
	Name string
	// target service
	Destination string
	// optional. Number of retained entries in stream. Flushing run every 1m.
	FlushAfter *int
	// optional. Heartbeat period for Redis Ping. Default to 10s
	RedisHeartbeat *time.Duration
	// optional. Number of concurrent call. Default to 10*runtime.NumCPU()
	MaxConcurrentCall *int
}

func NewClient(cfg ClientConfig, redisCfg redis.Options) *RpcClient {
	concurrencyLimit := 10 * runtime.NumCPU()
	if cfg.MaxConcurrentCall != nil {
		concurrencyLimit = *cfg.MaxConcurrentCall
	}
	heartbeat := 10 * time.Second
	if cfg.RedisHeartbeat != nil {
		heartbeat = *cfg.RedisHeartbeat
	}
	log.Println("<rpc-client> concurrency", concurrencyLimit)
	rpcInstance := RpcClient{
		client:       CreateRedisClient(heartbeat, &redisCfg),
		lastReadId:   "",
		replyChMap:   CreateSafeChannelMap(),
		name:         cfg.Name,
		dest:         cfg.Destination,
		replayStream: cfg.Name + "--reply-inbox",
		pool:         pond.New(concurrencyLimit, 100000),
	}
	// track reply stream
	go rpcInstance.replyStreamListener()
	// trim periodicaly
	go StreamCleanupScheduler(rpcInstance.client, rpcInstance.name, cfg.FlushAfter, nil)
	return &rpcInstance
}

func (r *RpcClient) replyStreamListener() {
	ctx := context.Background()
	streamSet := make([]string, 2)
	streamSet[0] = r.replayStream
	log.Printf("<rpc-listener> starting %s\n", r.replayStream)
	for {
		if r.lastReadId == "" {
			r.lastReadId = fmt.Sprint(time.Now().UnixMilli())
		}
		streamSet[1] = r.lastReadId
		// XREAD
		xreadResult := r.client.XRead(ctx, &redis.XReadArgs{
			Count:   int64(StreamFetchSize),
			Streams: streamSet,
		})
		// iterate items
		resultInStreams := xreadResult.Val()
		for _, aStream := range resultInStreams {
			for _, message := range aStream.Messages {
				// log.Println(message)
				r.lastReadId = message.ID
				msg := message
				go func() {
					idVal := fmt.Sprint(msg.Values["id"])
					keyId := "reply--" + fmt.Sprint(idVal)
					replyCh, exist := r.replyChMap.Get(idVal)
					if exist {
						// get from redis
						value, err := r.client.Get(ctx, keyId).Result()
						if err != nil {
							log.Printf("<rpc-reply-err> %s", err.Error())
						} else {
							// use reply channel
							rpcResp, _ := jsonrpc.ParseString(value)
							replyCh <- rpcResp
							close(replyCh)
						}
						r.replyChMap.Delete(idVal)
					}
				}()

			}
		}
	}
}

func (r *RpcClient) Call(ctx context.Context, method string, data interface{}) (*RpcObject, error) {
	// make result channel & context
	shortId := MessageIdGenerator()
	resultCh := make(chan RpcObject)
	// store channel
	r.replyChMap.Set(shortId, resultCh)
	// define local context
	timeout := 10 * time.Second // default timeout
	deadline, hasDeadline := ctx.Deadline()
	if hasDeadline {
		timeout = time.Until(deadline)
	}
	localCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	// submit request
	err := r.submitRequest(localCtx, shortId, method, data, false)
	if err != nil {
		return nil, err
	}
	select {
	case <-localCtx.Done():
		log.Println("<rpc-call> timeout", shortId)
		r.replyChMap.Delete(shortId) // delete from map
		// close(resultCh)              // close channel
		return nil, errors.New("Call timeout")
	case result := <-resultCh:
		// log.Println(result)
		return &result, nil
	}
}

func (r *RpcClient) Notify(ctx context.Context, method string, data interface{}) error {
	shortId := MessageIdGenerator()
	err := r.submitRequest(ctx, shortId, method, data, true)
	if err != nil {
		return err
	}
	return nil
}

func (r *RpcClient) submitRequest(ctx context.Context, id string, method string, data interface{}, isNotify bool) error {
	ctxTimeout, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	returnChannel := make(chan error)
	// submit to pool
	r.pool.Submit(func() {
		key := "request--" + id
		// lookup for deadline, default 30s
		timeout := 30 * time.Second
		deadline, valid := ctx.Deadline()
		if valid {
			// has deadline, use duration
			timeout = time.Until(deadline)
		}

		var setexMsg []byte
		var errRpc *jsonrpc.ErrorObj
		if isNotify {
			setexMsg, errRpc = jsonrpc.Notification(method, data)
		} else {
			setexMsg, errRpc = jsonrpc.Request(id, method, data)
		}
		if errRpc != nil {
			returnChannel <- errors.New(errRpc.Message)
			return
		}
		// setex
		if err := r.client.SetEX(ctx, key, setexMsg, timeout).Err(); err != nil {
			returnChannel <- err
			return
		}
		// add to inbox stream
		destInbox := r.dest + "--server-inbox"
		if err := r.client.XAdd(ctx, &redis.XAddArgs{
			Stream: destInbox,
			Values: map[string]interface{}{
				"id":      id,
				"replyTo": r.replayStream,
			},
		}).Err(); err != nil {
			returnChannel <- err
			return
		}
		// otherwise
		returnChannel <- nil
	})

	select {
	case retVal := <-returnChannel:
		return retVal
	case <-ctxTimeout.Done():
		return errors.New("submit timeout")
	}

}

func (r *RpcClient) End() {
	ctx := context.Background()
	if r.client != nil {
		if err := r.client.Ping(ctx).Err(); err == nil {
			r.client.Close()
		}
	}
}
