package commands

import (
	"fmt"

	"samyak.go_redis/store"
)

func HandleLPUSH(st *store.Store, parts []string) []byte {
	// LPUSH key element [element ...]
	if len(parts) < 3 {
		return []byte("-ERR wrong number of arguments\r\n")
	}

	key := parts[1]
	elements := parts[2:]

	length := st.LPush(key, elements)

	return []byte(fmt.Sprintf(":%d\r\n", length))
}
