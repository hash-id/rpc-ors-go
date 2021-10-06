package rpcorsgo

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/teambition/jsonrpc-go"
)

type HandlerFunc func(data interface{}) interface{}

type ReplyPayload interface{}

type RpcServer struct {
	client        *redis.Client
	name          string
	lastReadId    string
	registerChMap SafeReqObjMap
	bufferChSize  int
}

func NewService(name string) *RpcServer {
	rpcInstance := RpcServer{
		name:          name,
		client:        CreateRedisClient(),
		lastReadId:    fmt.Sprint(time.Now().UnixMilli()),
		registerChMap: CreateSafeReqObjMap(),
		bufferChSize:  10000,
	}
	go rpcInstance.inboxListener()
	return &rpcInstance
}

func (r *RpcServer) inboxListener() {
	ctx := context.Background()
	streamSet := make([]string, 1)
	streamSet[0] = r.name + "--server-inbox"
	for {
		// FIXME: XREADGROUP, sementara xread dulu sampai berfungsi
		xreadResult := r.client.XRead(ctx, &redis.XReadArgs{
			Count:   int64(defaultSize),
			Streams: streamSet,
			// Group:   r.name,
		})
		// iterate items
		resultInStreams := xreadResult.Val()
		for _, aStream := range resultInStreams {
			for _, message := range aStream.Messages {
				msgId := message.Values["id"]
				replyTo := message.Values["replyTo"]
				// get from redis
				payloadKey := "request--" + fmt.Sprint(msgId)
				payloadStr, err := r.client.Get(ctx, payloadKey).Result()
				if err != nil {
					continue
				}
				payload, _ := jsonrpc.ParseString(payloadStr)
				handlingCh, exist := r.registerChMap.Get(payload.Method)
				if exist {
					// kirim detail ke channel milik handler
					handlingCh <- RequestObj{
						id:      fmt.Sprint(msgId),
						replyTo: fmt.Sprint(replyTo),
						data:    payload,
					}
				}
			}
		}
	}
}

func (r *RpcServer) Register(method string, handler HandlerFunc) {
	handleCh := make(chan RequestObj, r.bufferChSize)
	go func() {
		for {
			incoming := <-handleCh
			go func() {
				result := handler(incoming.data)
				if incoming.data.Type == jsonrpc.NotificationType {
					return // is Notification only
				}
				// shall reply
				if err := r.sendReply(incoming, result); err != nil {
					log.Printf("[rpc-reply-error] %s", err.Error())
				}
			}()
		}
	}()
	//
	r.registerChMap.Set(method, handleCh)
}

func (r *RpcServer) sendReply(req RequestObj, data interface{}) error {
	ctx := context.Background()
	destination := req.replyTo
	msgId := req.id
	//
	// set payload
	resRpc, _ := jsonrpc.Success(msgId, data)
	key := "reply--" + msgId
	if err := r.client.SetEX(ctx, key, resRpc, 60*time.Second).Err(); err != nil {
		return err
	}
	// add to inbox stream
	requestDetail := ReplyObj{
		id: msgId,
	}
	if err := r.client.XAdd(ctx, &redis.XAddArgs{
		Stream: destination,
		Values: requestDetail,
	}).Err(); err != nil {
		return err
	}
	return nil
}
