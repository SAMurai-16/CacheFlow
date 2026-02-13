package store

import (
	"fmt"
	"strconv"
)

func (s *Store) Incr(key string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, exists := s.data[key]


	if !exists {
		s.data[key] = Entry{
		Type: StringType,
		Value: "1",
	}

		return 1,nil
	}

	if entry.Type != StringType {
		return 0, fmt.Errorf("ERR value is not an integer or out of range")
	}

	val, err := strconv.ParseInt(entry.Value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("ERR value is not an integer or out of range")
	}

	val++

	entry.Value = strconv.FormatInt(val, 10)
	s.data[key] = entry

	return val, nil
}
