package store


func (s *Store) XRead(key, id string) ([]StreamEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entry, exists := s.data[key]
	if !exists || entry.Type != StreamType {
		return []StreamEntry{}, nil
	}

	startMS, startSeq, err := parseStreamID(id)
	if err != nil {
		return nil, err
	}

	var result []StreamEntry

	for _, e := range entry.Stream {
		ms, seq, _ := parseStreamID(e.ID)

		// Exclusive comparison
		if ms > startMS || (ms == startMS && seq > startSeq) {
			result = append(result, e)
		}
	}

	return result, nil
}
