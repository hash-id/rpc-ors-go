package rpcorsgo

import (
	"github.com/go-redis/redis/v8"
)

const (
	defaultSize = 5
)

func CreateRedisClient() *redis.Client {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	return rdb
}
