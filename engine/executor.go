package engine

import (
	"fmt"
	"strings"

	"samyak.go_redis/commands"
	"samyak.go_redis/store"
)

func ExecuteCommands(st *store.Store, parts []string) []byte {

	cmd := strings.ToUpper(parts[0])

	switch cmd {
	case "PING":
		return ([]byte("+PONG\r\n"))

	case "ECHO":
		if len(parts) < 2 {
			return ([]byte("$0\r\n\r\n"))

		}
		arg := parts[1]
		return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg))

	case "REPLCONF":
		return []byte("+OK\r\n")

	case "SET":
		return commands.HandleSET(st, parts, true)

	case "GET":
		return commands.HandleGET(st, parts)

	case "RPUSH":
		return commands.HandleRPUSH(st, parts)

	case "LRANGE":
		return commands.HandleLRANGE(st, parts)

	case "LPUSH":
		return commands.HandleLPUSH(st, parts)

	case "LLEN":
		return commands.HandleLLEN(st, parts)

	case "LPOP":
		return commands.HandleLPOP(st, parts)

	case "TYPE":
		return commands.HandleTYPE(st, parts)

	case "XADD":
		return commands.HandleXADD(st, parts)

	case "XRANGE":
		return commands.HandleXRANGE(st, parts)

	case "XREAD":
		return commands.HandleXREAD(st, parts)

	case "INCR":
		return commands.HandleINCR(st, parts)
	case "INFO":
		return commands.HandleINFO(st, parts)

	case "WAIT":
		return commands.HandleWAIT(st, parts)

	case "CONFIG":
		return commands.HandleCONFIG(st, parts)
	case "KEYS":
		return commands.HANDLEKEYS(st,parts)


	default:
		return ([]byte("-ERR unknown command\r\n"))

	}

}
