package commands

import (
	"fmt"
	"strings"

	"samyak.go_redis/store"
)

func HandleCONFIG(st *store.Store, parts []string) []byte {
	if len(parts) != 3 {
		return []byte("-ERR wrong number of arguments for 'config|get' command\r\n")
	}

	if strings.ToUpper(parts[1]) != "GET" {
		return []byte("-ERR only CONFIG GET is supported\r\n")
	}

	param := strings.ToLower(parts[2])

	var value string
	switch param {
	case "dir":
		value = st.Dir
	case "dbfilename":
		value = st.DBFilename
	default:
		return []byte("*0\r\n")
	}

	return []byte(fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(param), param, len(value), value))
}
