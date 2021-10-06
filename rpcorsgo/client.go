package rpcorsgo

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/go-redis/redis/v8"
	gonanoid "github.com/matoous/go-nanoid/v2"
	jsonrpc "github.com/teambition/jsonrpc-go"
)

type RpcClient struct {
	client     *redis.Client
	name       string
	dest       string
	lastReadId string
	replyChMap SafeChannelMap
}

func NewClient(name string, dest string) *RpcClient {
	rpcInstance := RpcClient{
		client:     CreateRedisClient(),
		lastReadId: fmt.Sprint(time.Now().UnixMilli()),
		replyChMap: CreateSafeChannelMap(),
		name:       name,
		dest:       dest,
	}
	// spin reader
	go rpcInstance.replyStreamListener()
	return &rpcInstance
}

func (r *RpcClient) replyStreamListener() {
	ctx := context.Background()
	streamSet := make([]string, 1)
	streamSet[0] = r.name + "--reply"
	for {
		// XREAD
		xreadResult := r.client.XRead(ctx, &redis.XReadArgs{
			Count:   int64(defaultSize),
			Streams: streamSet,
		})
		// iterate items
		resultInStreams := xreadResult.Val()
		for _, aStream := range resultInStreams {
			for _, message := range aStream.Messages {
				idVal := message.Values["id"]
				ctxId := fmt.Sprint(idVal)
				replyCh, exist := r.replyChMap.Get(ctxId)
				if exist {
					// get from redis
					value, err := r.client.Get(ctx, ctxId).Result()
					if err != nil {
						log.Printf("[rpc-reply-err] %s", err.Error())
						// use reply channel
						rpcResp, _ := jsonrpc.ParseString(value)
						replyCh <- rpcResp
						close(replyCh)
					}
					r.replyChMap.Delete(ctxId)
				}
			}
		}
	}
}

func (r *RpcClient) Call(ctx context.Context, method string, data interface{}) (chan RpcObject, error) {
	shortId, err := r.wrapRequest(ctx, method, data, false)
	if err != nil {
		return nil, err
	}
	// make result channel
	resultCh := make(chan RpcObject)
	// store channel
	r.replyChMap.Set(shortId, resultCh)
	return resultCh, nil
}

func (r *RpcClient) Notify(ctx context.Context, method string, data interface{}) error {
	_, err := r.wrapRequest(ctx, method, data, true)
	if err != nil {
		return err
	}
	return nil
}

func (r *RpcClient) wrapRequest(ctx context.Context, method string, data interface{}, isNotify bool) (string, error) {
	shortId := gonanoid.MustGenerate("abcdefghijklmnopqrstuvwxyz12345678", 12)
	key := "request--" + shortId

	// lookup for deadline
	timeout := time.Second * 30 // default is 30s
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
		setexMsg, errRpc = jsonrpc.Request(shortId, method, data)
	}
	if errRpc != nil {
		return shortId, errors.New(errRpc.Message)
	}
	// setex
	if err := r.client.SetEX(ctx, key, setexMsg, timeout).Err(); err != nil {
		return shortId, err
	}
	// add to inbox stream
	destInbox := r.dest + "--server-inbox"
	replyInbox := r.name + "--reply-inbox"
	requestDetail := RequestObj{
		id:      shortId,
		replyTo: replyInbox,
	}
	if err := r.client.XAdd(ctx, &redis.XAddArgs{
		Stream: destInbox,
		Values: requestDetail,
	}).Err(); err != nil {
		return shortId, err
	}
	return shortId, nil
}

func (r *RpcClient) End() {
	ctx := context.Background()
	if r.client != nil {
		if err := r.client.Ping(ctx).Err(); err == nil {
			r.client.Close()
		}
	}
}
