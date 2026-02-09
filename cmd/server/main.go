package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strings"

	"samyak.go_redis/resp"
)

func main() {
	ln, err := net.Listen("tcp", ":6380")

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Connected to the server on port :6380")

	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}

		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
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
		default:
			conn.Write([]byte("unknown command"))

		}

	}
}
