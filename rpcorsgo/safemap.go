package rpcorsgo

import (
	"sync"

	"github.com/teambition/jsonrpc-go"
)

/* -------------------------------------------------------------------------- */
/*                                FOR CLIENT.GO                               */
/* -------------------------------------------------------------------------- */
type RpcObject *jsonrpc.RPC

type SafeChannelMap struct {
	records map[string]chan RpcObject
	mutex   sync.RWMutex
}

func CreateSafeChannelMap() SafeChannelMap {
	return SafeChannelMap{
		records: make(map[string]chan RpcObject),
	}
}

func (s *SafeChannelMap) Set(key string, value chan RpcObject) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.records[key] = value
}

func (s *SafeChannelMap) Delete(key string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	delete(s.records, key)
}

func (s *SafeChannelMap) Get(key string) (chan RpcObject, bool) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	value := s.records[key]
	if value == nil {
		return nil, false
	}
	return value, true
}

/* -------------------------------------------------------------------------- */
/*                                FOR SERVER.GO                               */
/* -------------------------------------------------------------------------- */
type ReplyObj struct {
	id string
}

type RequestObj struct {
	id      string
	replyTo string
	data    RpcObject
}

type SafeReqObjMap struct {
	records map[string]chan RequestObj
	mutex   sync.RWMutex
}

func CreateSafeReqObjMap() SafeReqObjMap {
	return SafeReqObjMap{
		records: make(map[string]chan RequestObj),
	}
}

func (s *SafeReqObjMap) Set(key string, value chan RequestObj) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.records[key] = value
}

func (s *SafeReqObjMap) Delete(key string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	delete(s.records, key)
}

func (s *SafeReqObjMap) Get(key string) (chan RequestObj, bool) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	value := s.records[key]
	if value == nil {
		return nil, false
	}
	return value, true
}
