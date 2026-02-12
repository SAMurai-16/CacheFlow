package store

import (
	"sync"
	"time"
)

type ValueType int

const (
	StringType ValueType = iota
	ListType
)

type Entry struct {
	Type   ValueType
	Value    string
	List    []string
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


func (s *Store) RPush(key string, elements []string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, exists := s.data[key]

	// Stage requirement: only handle non-existing key
	if !exists {
		s.data[key] = Entry{
			Type: ListType,
			List: append([]string{},elements...),
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

func (s * Store) LRange (key string, start, stop int) []string {
	s.mu.Lock()
	defer s.mu.Unlock()


	entry,exists := s.data[key]

	if !exists || entry.Type != ListType {
		return []string{}
	}

	list := entry.List
	n := len(list)

	if start>=n {
		return []string{}
	}

	if stop >= n { 
		stop = n-1
	}

	if start > stop {
		return []string{}
	}

	return list[start:stop+1]
}


func (s *Store) LPush (key string, elements []string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	reversed:= make([]string,0,len(elements))
	for i := len(elements) -1; i>=0;i--{
		reversed = append(reversed, elements[i])

	}

	entry, exists := s.data[key]

	 if !exists {
		s.data[key] = Entry{
			Type : ListType,
			List : reversed,
			
		}
		return len(reversed)
	 }

	if entry.Type == ListType{
		entry.List = append(reversed,entry.List...)
		s.data[key] = entry
		return len(entry.List)
	}

	return 0
}


func (s * Store) LLen (key string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entry, exists := s.data[key]

	if !exists {
		return 0
	}

	return len(entry.List)

}