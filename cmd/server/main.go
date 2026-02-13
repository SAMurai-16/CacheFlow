package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strings"

	"samyak.go_redis/commands"
	"samyak.go_redis/resp"
	"samyak.go_redis/store"
)

func main() {
	ln, err := net.Listen("tcp", ":6381")

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Connected to the server on port :6380")

	// Create a single store instance for all connections
	st := store.New()

	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}

		go handleConnection(conn, st)
	}
}

func handleConnection(conn net.Conn, st *store.Store) {
	defer conn.Close()


	inTransaction := false
	var queuedCommands [][]string


	fmt.Println("Client connected")

	reader := bufio.NewReader(conn)

	for {
		parts, err := resp.ReadRESPArray(reader)
		if err != nil {
			fmt.Println("Read error:", err)
			return
		}

		if len(parts) == 0 {
			continue
		}

		cmd := strings.ToUpper(parts[0])

		if cmd == "MULTI" {
			inTransaction = true
			fmt.Println(inTransaction)
			queuedCommands = nil
			conn.Write([]byte("+OK\r\n"))
			continue
		}

		if cmd == "EXEC"{
			fmt.Println(inTransaction)
			if !inTransaction {
				conn.Write([]byte("-ERR EXEC without MULTI\r\n"))
				continue
			}

			var response [][]byte

			for _,queued := range queuedCommands {
				resp:= executeCommands(st,queued)
				response = append(response, resp)
			}

			var result strings.Builder
			result.WriteString(fmt.Sprintf("*%d\r\n",len(response)))

			for _,r := range response{
				result.Write(r)
			}



			conn.Write([]byte(result.String()))

			inTransaction = false
			queuedCommands = nil
			continue
		}


		if inTransaction{
			queuedCommands = append(queuedCommands, parts)
			conn.Write([]byte("+QUEUED\r\n"))
			continue
		}

		resp := executeCommands(st,parts)
		conn.Write(resp)





	}
}


func executeCommands(st *store.Store, parts []string) []byte {

	cmd := strings.ToUpper(parts[0])

	switch cmd {
		case "PING":
			return ([]byte("+PONG\r\n"))

		case "ECHO":
			if len(parts) < 2 {
				return ([]byte("$0\r\n\r\n"))
			
			}
			arg := parts[1]
			return  []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg))
			

		case "SET":
			return commands.HandleSET(st, parts)
			

		case "GET":
			return  commands.HandleGET(st, parts)
			
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
			


		default:
		return ([]byte("-ERR unknown command\r\n"))

		}

}
