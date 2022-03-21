package storage

import (
	"errors"
	"hash/fnv"
	"sync"
)

var ErrKeyNotExist = errors.New("key doesn't exist")

type storage struct {
	sync.RWMutex
	m map[string]string // TODO: support more value types
}

func newStorage() *storage {
	return &storage{m: make(map[string]string)}
}

func (s *storage) put(key, value string) {
	s.Lock()
	s.m[key] = value
	s.Unlock()
}

func (s *storage) get(key string) (string, error) {
	s.RLock()
	value, ok := s.m[key]
	s.RUnlock()
	if !ok {
		return "", ErrKeyNotExist
	}
	return value, nil
}

func (s *storage) del(key string) {
	s.Lock()
	delete(s.m, key)
	s.Unlock()
}

type Storage []*storage

func NewStorage(n uint32) Storage {
	s := make(Storage, n)
	for i := uint32(0); i < n; i++ {
		s[i] = newStorage()
	}
	return s
}

func (s Storage) getBucketIdx(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32() % uint32(len(s))
}

func (s Storage) Put(key, value string) error {
	s[s.getBucketIdx(key)].put(key, value)
	return nil
}

func (s Storage) Get(key string) (string, error) {
	return s[s.getBucketIdx(key)].get(key)
}

func (s Storage) Del(key string) error {
	s[s.getBucketIdx(key)].del(key)
	return nil
}
