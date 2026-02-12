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

		switch cmd {
		case "PING":
			conn.Write([]byte("+PONG\r\n"))

		case "ECHO":
			if len(parts) < 2 {
				conn.Write([]byte("$0\r\n\r\n"))
				continue
			}
			arg := parts[1]
			resp := fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg)
			conn.Write([]byte(resp))

		case "SET":
			resp := commands.HandleSET(st, parts)
			conn.Write(resp)

		case "GET":
			resp := commands.HandleGET(st, parts)
			conn.Write(resp)
		case "RPUSH":
			resp := commands.HandleRPUSH(st, parts)
			conn.Write(resp)
		case "LRANGE": 
			resp := commands.HandleLRANGE(st,parts)
			conn.Write(resp)
		case "LPUSH":
			resp := commands.HandleLPUSH(st, parts)
			conn.Write(resp)
		case "LLEN":
			resp := commands.HandleLLEN(st,parts)
			conn.Write(resp)


		default:
			conn.Write([]byte("unknown command"))

		}

	}
}
