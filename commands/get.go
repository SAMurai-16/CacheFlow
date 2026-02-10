package commands

import (
	"fmt"

	"samyak.go_redis/store"
)

func HandleGET(st *store.Store, parts []string) []byte {
	if len(parts) < 2 {
		return []byte("-ERR wrong number of arguments for 'get' command\r\n")
	}

	key := parts[1]

	value, ok := st.Get(key)
	if !ok {
		return ([]byte("$-1\r\n"))
	}

	return ([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)))
}
