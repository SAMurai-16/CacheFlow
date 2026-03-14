package commands

import (
	"fmt"
	"sort"

	"samyak.go_redis/store"
)

func HANDLEKEYS(st *store.Store, parts []string) []byte {
	if len(parts) != 2 {
		return []byte("-ERR wrong number of arguments for 'keys' command\r\n")

	}

	if parts[1] != "*" {
		return []byte("*0\r\n")
	}

	keys := st.Keys()
	sort.Strings(keys)

	resp := fmt.Sprintf("*%d\r\n", len(keys))
	for _, k := range keys {
		resp += fmt.Sprintf("$%d\r\n%s\r\n", len(k), k)
	}
	return []byte(resp)

}
