package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"

	"samyak.go_redis/engine"
	"samyak.go_redis/helper"
	"samyak.go_redis/resp"
	"samyak.go_redis/store"
)

var emptyRDB = []byte{
	0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x30,
	0x39, 0xfa, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00,
}

func main() {
	port := "6380"

	role := "master"
	masterHost := ""
	masterPort := ""
	dir := "."
	dbfilename := "dump.rdb"

	for i := 0; i < len(os.Args); i++ {
		if os.Args[i] == "--port" && i+1 < len(os.Args) {
			port = os.Args[i+1]

		}

		if os.Args[i] == "--replicaof" && i+2 < len(os.Args) {
			role = "slave"
			masterHost = os.Args[i+1]
			masterPort = os.Args[i+2]
		}

		if os.Args[i] == "--dir" && i+1 < len(os.Args) {
			dir = os.Args[i+1]
		}

		if os.Args[i] == "--dbfilename" && i+1 < len(os.Args) {
			dbfilename = os.Args[i+1]
		}
		}

	ln, err := net.Listen("tcp", ":"+port)

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Connected to the server on port", port)

	// Create a single store instance for all connections
	st := store.New(role, masterHost, masterPort, port, dir, dbfilename)

	if err := st.LoadRDB(); err != nil {
    fmt.Println("Failed to load RDB:", err)
}

	if st.Role == "slave" {
		go helper.ConnectToMaster(st)
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}

		go handleConnection(conn, st)
	}
}

func handleConnection(conn net.Conn, st *store.Store) {
	defer func() {
		removeReplicaConnection(st, conn)
		conn.Close()
	}()

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

		if cmd == "REPLCONF" && len(parts) >= 3 && strings.ToUpper(parts[1]) == "ACK" {
			offset, parseErr := strconv.ParseInt(parts[2], 10, 64)
			if parseErr == nil {
				st.Mu.Lock()
				st.ReplicaAck[conn] = offset
				st.Mu.Unlock()
			}
			continue
		}

		if cmd == "MULTI" {
			inTransaction = true
			fmt.Println(inTransaction)
			queuedCommands = nil
			conn.Write([]byte("+OK\r\n"))
			continue
		}

		if cmd == "EXEC" {
			fmt.Println(inTransaction)
			if !inTransaction {
				conn.Write([]byte("-ERR EXEC without MULTI\r\n"))
				continue
			}

			var response [][]byte

			for _, queued := range queuedCommands {
				resp := engine.ExecuteCommands(st, queued)
				response = append(response, resp)
			}

			var result strings.Builder
			result.WriteString(fmt.Sprintf("*%d\r\n", len(response)))

			for _, r := range response {
				result.Write(r)
			}

			conn.Write([]byte(result.String()))

			inTransaction = false
			queuedCommands = nil
			continue
		}

		if cmd == "DISCARD" {
			if !inTransaction {
				conn.Write([]byte("-ERR DISCARD without MULTI\r\n"))
				continue
			}

			inTransaction = false
			queuedCommands = nil

			conn.Write([]byte("+OK\r\n"))
			continue
		}

		if cmd == "PSYNC" {

			// 1) Send FULLRESYNC
			full := fmt.Sprintf(
				"+FULLRESYNC %s %d\r\n",
				st.ReplID,
				st.ReplOffset,
			)

			conn.Write([]byte(full))

			// 2) Send RDB snapshot
			header := fmt.Sprintf("$%d\r\n", len(emptyRDB))
			conn.Write([]byte(header))

			conn.Write(emptyRDB)

			// 3) register the replica
			st.Mu.Lock()
			st.Replicas = append(st.Replicas, conn)
			st.ReplicaAck[conn] = 0
			st.Mu.Unlock()

			continue
		}

		if inTransaction {
			queuedCommands = append(queuedCommands, parts)
			conn.Write([]byte("+QUEUED\r\n"))
			continue
		}

		resp := engine.ExecuteCommands(st, parts)
		conn.Write(resp)

		if st.Role == "master" && shouldPropagateCommand(cmd) {
			helper.PropagateToReplicas(st, parts)
		}

	}
}

func shouldPropagateCommand(cmd string) bool {
	switch cmd {
	case "SET", "INCR", "LPUSH", "RPUSH", "LPOP", "XADD":
		return true
	default:
		return false
	}
}

func removeReplicaConnection(st *store.Store, conn net.Conn) {
	st.Mu.Lock()
	defer st.Mu.Unlock()

	for i, replica := range st.Replicas {
		if replica == conn {
			st.Replicas = append(st.Replicas[:i], st.Replicas[i+1:]...)
			break
		}
	}

	delete(st.ReplicaAck, conn)
}
