package store

import (
	"sync"
	"time"
)

type ValueType int

const (
	StringType ValueType = iota
	ListType
	StreamType
)

type StreamEntry struct {
	ID     string
	Fields map[string]string
}

type Entry struct {
	Type     ValueType
	Value    string
	List     []string
	Stream   []StreamEntry
	ExpireAt time.Time
}

type Store struct {
	mu   sync.RWMutex
	data map[string]Entry

	
	Role string   // "master" or "slave"

	MasterHost string
	MasterPort string


	ReplID     string
	ReplOffset int64

	ReplicaPort string
}

func New(role, host, masterPort, replicaPort string) *Store {
	return &Store{
		data: make(map[string]Entry),
		Role: role,
		MasterHost: host,
		MasterPort: masterPort,
		ReplicaPort: replicaPort,
		ReplID:       "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
		ReplOffset:   0,

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

func (s *Store) RPush(key string, elements []string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, exists := s.data[key]

	// Stage requirement: only handle non-existing key
	if !exists {
		s.data[key] = Entry{
			Type: ListType,
			List: append([]string{}, elements...),
		}
		return len(elements)
	}

	if entry.Type == ListType {
		entry.List = append(entry.List, elements...)
		s.data[key] = entry
		return len(entry.List)
	}

	return 0
}

func (s *Store) LRange(key string, start, stop int) []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, exists := s.data[key]

	if !exists || entry.Type != ListType {
		return []string{}
	}

	list := entry.List
	n := len(list)

	if start >= n {
		return []string{}
	}

	if stop >= n {
		stop = n - 1
	}

	if start > stop {
		return []string{}
	}

	return list[start : stop+1]
}

func (s *Store) LPush(key string, elements []string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	reversed := make([]string, 0, len(elements))
	for i := len(elements) - 1; i >= 0; i-- {
		reversed = append(reversed, elements[i])

	}

	entry, exists := s.data[key]

	if !exists {
		s.data[key] = Entry{
			Type: ListType,
			List: reversed,
		}
		return len(reversed)
	}

	if entry.Type == ListType {
		entry.List = append(reversed, entry.List...)
		s.data[key] = entry
		return len(entry.List)
	}

	return 0
}

func (s *Store) LLen(key string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entry, exists := s.data[key]

	if !exists {
		return 0
	}

	return len(entry.List)

}

func (s *Store) LPop(key string, count int) []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, exists := s.data[key]
	if !exists || entry.Type != ListType || len(entry.List) == 0 {
		return []string{}
	}

	if count <= 0 {
		return []string{}
	}

	if count > len(entry.List) {
		count = len(entry.List)
	}

	removed := entry.List[:count]
	entry.List = entry.List[count:]

	if len(entry.List) == 0 {
		delete(s.data, key)

	} else {
		s.data[key] = entry
	}

	result := make([]string, len(removed))
	copy(result, removed)
	return result
}

func (s *Store) TypeOf(key string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entry, exists := s.data[key]
	if !exists {
		return "none"
	}

	switch entry.Type {
	case StringType:
		return "string"
	case ListType:
		return "list"
	case StreamType:
	return "stream"
	default:
		return "none"
	}
}



