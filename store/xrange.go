package store

import (
	"math"
	"strconv"
	"strings"
)

func normalizeRangeID(id string, isStart bool) (int64, int64, error) {

	if id == "-" {
		return 0, 0, nil
	}



	if id == "+" {
		return math.MaxInt64, math.MaxInt64, nil
	}

	if strings.Contains(id, "-") {
		return parseStreamID(id)
	}

	// No sequence provided
	ms, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		return 0, 0, err
	}

	if isStart {
		return ms, 0, nil
	}

	return ms, math.MaxInt64, nil
}

func (s *Store) XRange(key, startID, endID string) ([]StreamEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entry, exists := s.data[key]
	if !exists || entry.Type != StreamType {
		return []StreamEntry{}, nil
	}

	startMS, startSeq, err := normalizeRangeID(startID, true)
	if err != nil {
		return nil, err
	}

	endMS, endSeq, err := normalizeRangeID(endID, false)
	if err != nil {
		return nil, err
	}

	var result []StreamEntry

	for _, e := range entry.Stream {
		ms, seq, _ := parseStreamID(e.ID)

		if (ms > startMS || (ms == startMS && seq >= startSeq)) &&
			(ms < endMS || (ms == endMS && seq <= endSeq)) {
			result = append(result, e)
		}
	}

	return result, nil
}
