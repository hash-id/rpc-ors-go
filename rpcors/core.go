package rpcors

import (
	"context"
	"crypto/md5"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	gonanoid "github.com/matoous/go-nanoid/v2"
)

const (
	StreamFetchSize = 100
)

func CreateRedisClient(heartbeat time.Duration, cfg *redis.Options) *redis.Client {
	redisCfg := redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	}
	if cfg != nil {
		redisCfg = *cfg
	}
	rdb := redis.NewClient(&redisCfg)
	// setup heartbeat
	go runHeartBeat(rdb, heartbeat)
	return rdb
}

func runHeartBeat(cl *redis.Client, duration time.Duration) {
	ctx := context.Background()
	ticker := time.NewTicker(duration)
	for {
		<-ticker.C // blocking
		if err := cl.Ping(ctx).Err(); err != nil {
			log.Println("<rpc-heartbeat> ping failed: ", err.Error())
		}
	}
}

func StreamCleanupScheduler(cl *redis.Client, stream string, flushSize *int, svc *RpcServer) {
	ctx := context.Background()
	ticker := time.NewTicker(1 * time.Minute)
	maxLen := 50000 //default 50k
	if flushSize != nil {
		maxLen = *flushSize
	}
	for {
		<-ticker.C // blocking
		// both server and client doing XTRIM
		if err := cl.XTrimMaxLenApprox(ctx, stream, int64(maxLen), 0).Err(); err != nil {
			log.Printf("<rpc-trim> %s failure: %s", stream, err.Error())
		}
		// this section is only on server --> consumers removal
		if svc != nil {
			fiveMinutesAgo := time.Now().Add(-5 * time.Minute).UnixMilli()
			oldConsumers, _ := cl.ZRangeByScore(ctx, svc.hashedName, &redis.ZRangeBy{
				Max: fmt.Sprint(fiveMinutesAgo),
			}).Result()
			log.Println(oldConsumers)
			for _, name := range oldConsumers {
				// delete old consumer from consumer group
				if err := cl.XGroupDelConsumer(ctx, svc.inboxStream, consumerGroupName, name).Err(); err != nil {
					log.Println("XGroupDelConsumer", err.Error())
				}
			}
			if len(oldConsumers) > 0 {
				// remove entries in sorted sets
				if err := cl.ZRemRangeByScore(ctx, svc.hashedName, "0", fmt.Sprint(fiveMinutesAgo)).Err(); err != nil {
					log.Println("ZRemRangeByScore", err.Error())
				}
			}
		}
	}
}

/* ---------- ex: KUG59LPK-5BC, KUG59Y2T-C6D, KUG5AABS-BC3 ---------- */
func MessageIdGenerator() string {
	nowInMs := time.Now().UnixMilli()
	timeId := strings.ToUpper(strconv.FormatInt(nowInMs, 36))
	shortid := gonanoid.MustGenerate("01234567890ABCDEFGHIJKLMNOPQRSTUVWXYZ", 6)
	finalId := timeId + "-" + shortid
	// fmt.Println(finalId)
	return finalId
}

func ConsumerNameGenerator(baseName string) string {
	shortid := gonanoid.MustGenerate("01234567890ABCDEFGHIJKLMNOPQRSTUVWXYZ", 6)
	finalId := baseName + "-" + shortid
	return finalId
}

func NameToMd5(name string) string {
	m := md5.New()
	m.Write([]byte(name))
	bs := m.Sum(nil)
	result := fmt.Sprintf("%x", bs)
	return result
}
