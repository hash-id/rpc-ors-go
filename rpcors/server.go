package rpcors

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/teambition/jsonrpc-go"
)

const (
	consumerGroupName = "reply-consumer"
)

type HandlerFunc func(data *jsonrpc.RPC) ([]byte, *jsonrpc.ErrorObj)

type RpcServer struct {
	client        *redis.Client
	name          string
	sortedSetName string
	registerChMap SafeReqObjMap
	bufferChSize  int
	inboxStream   string
	consumerName  string
}

type ServiceConfig struct {
	// current service (self)
	Name string
	// optional. Number of retained entries in stream. Flushing run every 1m.
	FlushAfter *int
	// optional. Heartbeat period for Redis Ping. Default to 10s
	RedisHeartbeat *time.Duration
}

func NewService(cfg ServiceConfig, redisCfg redis.Options) *RpcServer {
	heartbeat := 10 * time.Second
	if cfg.RedisHeartbeat != nil {
		heartbeat = *cfg.RedisHeartbeat
	}
	rpcInstance := RpcServer{
		name:          cfg.Name,
		sortedSetName: NameToMd5(cfg.Name),
		client:        CreateRedisClient(heartbeat, &redisCfg),
		registerChMap: CreateSafeReqObjMap(),
		bufferChSize:  10000,
		inboxStream:   cfg.Name + "--server-inbox",
	}
	rpcInstance.prepareConsumerGroup()
	// start listener
	go rpcInstance.inboxListener()
	// cl periodicaly
	go StreamCleanupScheduler(rpcInstance.client, rpcInstance.inboxStream, cfg.FlushAfter, &rpcInstance)
	return &rpcInstance
}

func (r *RpcServer) prepareConsumerGroup() {
	ctx := context.Background()
	// setup consumerGroupName if not exist
	list, err := r.client.XInfoGroups(ctx, r.inboxStream).Result()
	if err != nil {
		log.Println(err)
	}
	readerExist := false
	for _, v := range list {
		if v.Name == consumerGroupName {
			readerExist = true
		}
	}
	if !readerExist {
		_, err := r.client.XGroupCreate(ctx, r.inboxStream, consumerGroupName, "0").Result()
		if err != nil {
			log.Println("XGroup error", r.inboxStream, err)
		} else {
			log.Println("XGroup created", r.inboxStream, consumerGroupName)
		}
	} else {
		log.Println("XGroup exist", r.inboxStream, consumerGroupName)
	}
	// setup consumer name
	r.consumerName = ConsumerNameGenerator(r.name)
}

func (r *RpcServer) inboxListener() {
	log.Printf("<rpc-listener> starting %s\n", r.inboxStream)
	log.Println(r.consumerName, r.sortedSetName)
	ctx := context.Background()
	for {
		// update current consumer name to sorted-set, with current unixtime as score
		err := r.client.ZAdd(ctx, r.sortedSetName, &redis.Z{Score: float64(time.Now().UnixMilli()), Member: r.consumerName}).Err()
		if err != nil {
			fmt.Println("ZAdd failed", r.consumerName)
		}
		// XREADGROUP
		xstreamList, err := r.client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Count:    int64(StreamFetchSize),
			Streams:  []string{r.inboxStream, ">"},
			Group:    consumerGroupName,
			Consumer: r.consumerName,
		}).Result()
		if err != nil {
			log.Println(err.Error())
		}

		// iterate items
		for _, aStream := range xstreamList {
			for _, message := range aStream.Messages {
				streamMsg := message
				// use goroutine for faster loop!
				go func() {
					msgId := streamMsg.Values["id"]
					replyTo := streamMsg.Values["replyTo"]
					// get from redis
					payloadKey := "request--" + fmt.Sprint(msgId)
					payloadStr, err := r.client.Get(ctx, payloadKey).Result()
					if err != nil {
						return
					}
					payload, _ := jsonrpc.ParseString(payloadStr)
					handlingCh, exist := r.registerChMap.Get(payload.Method)
					if exist {
						// send detail to handler's channel
						handlingCh <- RequestObj{
							id:      fmt.Sprint(msgId),
							replyTo: fmt.Sprint(replyTo),
							data:    payload,
						}
					}
					// confirm consumption with XACK
					r.client.XAck(ctx, r.inboxStream, consumerGroupName, fmt.Sprint(streamMsg.ID))
				}()

			}
		}
	}
}

func (r *RpcServer) Register(method string, handler HandlerFunc) {
	handleCh := make(chan RequestObj, r.bufferChSize)
	go func() {
		log.Printf("<rpc-server> register=> %s", method)
		for {
			incoming := <-handleCh
			// spin another go routine for faster loop
			go func() {
				rpcReply, errHandle := handler(incoming.data)
				if incoming.data.Type == jsonrpc.NotificationType {
					return // is Notification only
				}
				// shall reply
				if errHandle != nil {
					// type casting pointer
					rpcReply, _ = jsonrpc.Error(incoming.id, (*jsonrpc.ErrorObj)(errHandle))
				}
				if err := r.sendReply(incoming, rpcReply); err != nil {
					log.Printf("<rpc-reply-error> %s\n", err.Error())
				}
			}()
		}
	}()
	//
	r.registerChMap.Set(method, handleCh)
}

func (r *RpcServer) sendReply(req RequestObj, data []byte) error {
	ctx := context.Background()
	destination := req.replyTo
	msgId := req.id
	//
	// set payload
	key := "reply--" + msgId
	if err := r.client.SetEX(ctx, key, data, 60*time.Second).Err(); err != nil {
		return err
	}
	// add to inbox stream
	if err := r.client.XAdd(ctx, &redis.XAddArgs{
		Stream: destination,
		Values: map[string]interface{}{"id": msgId},
	}).Err(); err != nil {
		return err
	}
	return nil
}
