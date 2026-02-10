package store

import (
	"sync"
	"time"
)

type Entry struct {
	Value    string
	ExpireAt time.Time
}

type Store struct {
	mu   sync.RWMutex
	data map[string]Entry
}

func New() *Store {
	return &Store{
		data: make(map[string]Entry),
	}
}

func (s *Store) Set(key, value string, expireAt time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data[key] = Entry{
		Value:    value,
		ExpireAt: expireAt,
	}

}

func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entry, ok := s.data[key]
	if !ok {
		return "", false
	}

	if !entry.ExpireAt.IsZero() && time.Now().After(entry.ExpireAt) {
		delete(s.data, key)
		return "", false

	}

	return entry.Value, true
}
