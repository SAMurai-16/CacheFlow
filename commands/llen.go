package commands

import (
	"fmt"

	"samyak.go_redis/store"
)

func HandleLLEN(st *store.Store, parts []string) []byte {
	// LLEN key
	if len(parts) < 2 {
		return []byte("-ERR wrong number of arguments\r\n")
	}

	key := parts[1]
	length := st.LLen(key)

	return []byte(fmt.Sprintf(":%d\r\n", length))
}
