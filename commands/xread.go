package commands

import (
	"fmt"
	"strings"

	"samyak.go_redis/store"
)

func HandleXREAD(st *store.Store, parts []string) []byte {
	if len(parts) < 4 {
		return []byte("-ERR wrong number of arguments\r\n")
	}

	if strings.ToUpper(parts[1]) != "STREAMS" {
		return []byte("-ERR syntax error\r\n")
	}

	args := parts[2:]

	if len(args)%2 != 0 {
		return []byte("-ERR wrong number of arguments\r\n")
	}

	n := len(args) / 2

	keys := args[:n]
	ids := args[n:]

	var resp strings.Builder
	streamCount := 0

	var streamResponses []string

	for i := 0; i < n; i++ {
		key := keys[i]
		id := ids[i]

		entries, err := st.XRead(key, id)
		if err != nil {
			return []byte("-ERR invalid id\r\n")
		}

		if len(entries) == 0 {
			continue
		}

		var streamResp strings.Builder

		streamResp.WriteString("*2\r\n")

		// Stream key
		streamResp.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(key), key))

		// Entries array
		streamResp.WriteString(fmt.Sprintf("*%d\r\n", len(entries)))

		for _, e := range entries {
			streamResp.WriteString("*2\r\n")

			// Entry ID
			streamResp.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(e.ID), e.ID))

			// Field-value array
			streamResp.WriteString(fmt.Sprintf("*%d\r\n", len(e.Fields)*2))

			for field, value := range e.Fields {
				streamResp.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(field), field))
				streamResp.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(value), value))
			}
		}

		streamResponses = append(streamResponses, streamResp.String())
		streamCount++
	}

	if streamCount == 0 {
		return []byte("*0\r\n")
	}

	resp.WriteString(fmt.Sprintf("*%d\r\n", streamCount))
	for _, s := range streamResponses {
		resp.WriteString(s)
	}

	return []byte(resp.String())
}
