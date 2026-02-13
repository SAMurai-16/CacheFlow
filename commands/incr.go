package commands

import (
	"fmt"

	"samyak.go_redis/store"
)

func HandleINCR(st *store.Store, parts []string) []byte {
	if len(parts) < 2 {
		return []byte("-ERR wrong number of arguments\r\n")
	}

	key := parts[1]

	newVal, err := st.Incr(key)
	if err != nil {
		return []byte("-ERR invalid value\r\n")
	}

	return []byte(fmt.Sprintf(":%d\r\n", newVal))
}
