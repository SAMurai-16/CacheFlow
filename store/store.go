package store

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
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
	Mu   sync.RWMutex
	data map[string]Entry

	Role string // "master" or "slave"

	MasterHost string
	MasterPort string

	ReplID     string
	ReplOffset int64

	ReplicaAck     map[net.Conn]int64
	LastWaitOffset int64

	ReplicaPort string
	Replicas    []net.Conn

	Dir        string
	DBFilename string

	AppendOnly     string
	AppendDirname  string
	AppendFilename string
	AppendFsync    string

	ChannelSubscribers map[string]map[net.Conn]struct{}
}

func New(role, host, masterPort, replicaPort, dir, dbfilename, appendonly, appenddirname, appendfilename, appendfsync string) *Store {
	return &Store{
		data:               make(map[string]Entry),
		Role:               role,
		MasterHost:         host,
		MasterPort:         masterPort,
		ReplicaPort:        replicaPort,
		ReplID:             "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
		ReplOffset:         0,
		ReplicaAck:         make(map[net.Conn]int64),
		LastWaitOffset:     0,
		Replicas:           make([]net.Conn, 0),
		Dir:                dir,
		DBFilename:         dbfilename,
		AppendOnly:         appendonly,
		AppendDirname:      appenddirname,
		AppendFilename:     appendfilename,
		AppendFsync:        appendfsync,
		ChannelSubscribers: make(map[string]map[net.Conn]struct{}),
	}
}

func (s *Store) EnsureAppendOnlyDir() error {
	if s.AppendOnly != "yes" {
		return nil
	}

	return os.MkdirAll(filepath.Join(s.Dir, s.AppendDirname), 0755)
}

func (s *Store) EnsureAppendOnlyFile() error {
	if s.AppendOnly != "yes" {
		return nil
	}

	aofPath := filepath.Join(s.Dir, s.AppendDirname, s.AppendFilename+".1.incr.aof")
	if _, err := os.Stat(aofPath); os.IsNotExist(err) {
		file, err := os.Create(aofPath)
		if err != nil {
			return err
		}
		file.Close()
	}

	manifestPath := filepath.Join(s.Dir, s.AppendDirname, s.AppendFilename+".manifest")
	if _, err := os.Stat(manifestPath); os.IsNotExist(err) {
		manifestFile, err := os.Create(manifestPath)
		if err != nil {
			return err
		}
		defer manifestFile.Close()

		manifestContent := "file " + s.AppendFilename + ".1.incr.aof seq 1 type i\n"
		_, err = manifestFile.WriteString(manifestContent)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Store) AppendToAOF(parts []string) error {
	if s.AppendOnly != "yes" {
		return nil
	}

	manifestPath := filepath.Join(s.Dir, s.AppendDirname, s.AppendFilename+".manifest")
	content, err := os.ReadFile(manifestPath)
	if err != nil {
		return err
	}

	var aofFileName string
	lines := strings.Split(string(content), "\n")
	for _, line := range lines {
		if strings.Contains(line, "type i") {
			tokens := strings.Split(line, " ")
			for i, token := range tokens {
				if token == "file" && i+1 < len(tokens) {
					aofFileName = tokens[i+1]
					break
				}
			}
		}
	}

	if aofFileName == "" {
		return nil
	}

	aofPath := filepath.Join(s.Dir, s.AppendDirname, aofFileName)
	f, err := os.OpenFile(aofPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("*%d\r\n", len(parts)))
	for _, p := range parts {
		builder.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(p), p))
	}
	
	_, err = f.WriteString(builder.String())
	if err != nil {
		return err
	}
	
	if s.AppendFsync == "always" {
		f.Sync()
	}

	return nil
}

func (s *Store) Set(key, value string, expireAt time.Time) {
	s.Mu.Lock()
	defer s.Mu.Unlock()

	s.data[key] = Entry{
		Value:    value,
		ExpireAt: expireAt,
	}

}

func (s *Store) Get(key string) (string, bool) {
	s.Mu.Lock()
	defer s.Mu.Unlock()

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
	s.Mu.Lock()
	defer s.Mu.Unlock()

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
	s.Mu.Lock()
	defer s.Mu.Unlock()

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
	s.Mu.Lock()
	defer s.Mu.Unlock()

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
	s.Mu.RLock()
	defer s.Mu.RUnlock()

	entry, exists := s.data[key]

	if !exists {
		return 0
	}

	return len(entry.List)

}

func (s *Store) LPop(key string, count int) []string {
	s.Mu.Lock()
	defer s.Mu.Unlock()

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
	s.Mu.RLock()
	defer s.Mu.RUnlock()

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

func (s *Store) Keys() []string {
	s.Mu.Lock()
	defer s.Mu.Unlock()

	now := time.Now()
	out := make([]string, 0, len(s.data))
	for k, e := range s.data {
		if !e.ExpireAt.IsZero() && now.After(e.ExpireAt) {
			delete(s.data, k)
			continue
		}
		out = append(out, k)
	}
	return out
}

func (s *Store) AddSubscriber(channel string, conn net.Conn) {
	s.Mu.Lock()
	defer s.Mu.Unlock()

	if s.ChannelSubscribers[channel] == nil {
		s.ChannelSubscribers[channel] = make(map[net.Conn]struct{})
	}
	s.ChannelSubscribers[channel][conn] = struct{}{}
}

func (s *Store) CountSubscribers(channel string) int {
	s.Mu.RLock()
	defer s.Mu.RUnlock()

	return len(s.ChannelSubscribers[channel])
}

func (s *Store) RemoveSubscriber(channel string, conn net.Conn) {
	s.Mu.Lock()
	defer s.Mu.Unlock()

	subs, ok := s.ChannelSubscribers[channel]
	if !ok {
		return
	}
	delete(subs, conn)
	if len(subs) == 0 {
		delete(s.ChannelSubscribers, channel)
	}
}

func (s *Store) Subscribers(channel string) []net.Conn {
	s.Mu.RLock()
	defer s.Mu.RUnlock()

	subs := s.ChannelSubscribers[channel]
	out := make([]net.Conn, 0, len(subs))
	for c := range subs {
		out = append(out, c)
	}
	return out
}
