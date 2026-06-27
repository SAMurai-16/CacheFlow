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
	//setting default values
	port := "6380"

	role := "master"
	masterHost := ""
	masterPort := ""
	dir, err := os.Getwd()
	if err != nil {
		dir = "."
	}
	dbfilename := "dump.rdb"
	appendonly := "no"
	appenddirname := "appendonlydir"
	appendfilename := "appendonly.aof"
	appendfsync := "everysec"

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

		if os.Args[i] == "--appendonly" && i+1 < len(os.Args) {
			appendonly = os.Args[i+1]
		}

		if os.Args[i] == "--appenddirname" && i+1 < len(os.Args) {
			appenddirname = os.Args[i+1]
		}

		if os.Args[i] == "--appendfilename" && i+1 < len(os.Args) {
			appendfilename = os.Args[i+1]
		}

		if os.Args[i] == "--appendfsync" && i+1 < len(os.Args) {
			appendfsync = os.Args[i+1]
		}
	}

	ln, err := net.Listen("tcp", ":"+port)

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Connected to the server on port", port)

	// Create a single store instance for all connections
	st := store.New(role, masterHost, masterPort, port, dir, dbfilename, appendonly, appenddirname, appendfilename, appendfsync)

	if err := st.EnsureAppendOnlyDir(); err != nil {
		fmt.Println("Failed to create append-only directory:", err)
	}

	if err := st.EnsureAppendOnlyFile(); err != nil {
		fmt.Println("Failed to create append-only file:", err)
	}

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

	inTransaction := false
	var queuedCommands [][]string
	subscriptions := make(map[string]struct{})

	defer func() {
		removeReplicaConnection(st, conn)
		for ch := range subscriptions {
			st.RemoveSubscriber(ch, conn)
		}
		conn.Close()
	}()

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

		if len(subscriptions) > 0 && !isAllowedInSubscribedMode(cmd) {
			errResp := fmt.Sprintf(
				"-ERR Can't execute '%s' in subscribed mode\r\n",
				strings.ToLower(cmd),
			)
			conn.Write([]byte(errResp))
			continue
		}

		if cmd == "PING" && len(subscriptions) > 0 {
			conn.Write([]byte("*2\r\n$4\r\npong\r\n$0\r\n\r\n"))
			continue
		}

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

		if cmd == "SUBSCRIBE" {
			if len(parts) < 2 {
				conn.Write([]byte("-ERR wrong number of arguments for 'subscribe' command\r\n"))
				continue
			}

			for _, channel := range parts[1:] {
				subscriptions[channel] = struct{}{} // unique channels per client
				st.AddSubscriber(channel, conn)
				count := len(subscriptions)

				resp := fmt.Sprintf(
					"*3\r\n$9\r\nsubscribe\r\n$%d\r\n%s\r\n:%d\r\n",
					len(channel),
					channel,
					count,
				)
				conn.Write([]byte(resp))
			}
			continue
		}

		if cmd == "PUBLISH" {
			if len(parts) != 3 {
				conn.Write([]byte("-ERR wrong number of arguments for 'publish' command\r\n"))
				continue
			}

			channel := parts[1]
			message := parts[2]

			subs := st.Subscribers(channel)

			push := fmt.Sprintf(
				"*3\r\n$7\r\nmessage\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
				len(channel), channel, len(message), message,
			)

			delivered := 0
			for _, subConn := range subs {
				if _, werr := subConn.Write([]byte(push)); werr == nil {
					delivered++
				}
			}

			conn.Write([]byte(fmt.Sprintf(":%d\r\n", delivered)))
			continue
		}

		if cmd == "UNSUBSCRIBE" {
			// For this stage, tester sends channel names; handle empty too for safety.
			if len(parts) == 1 {
				// Redis unsubscribes from all current channels.
				for channel := range subscriptions {
					delete(subscriptions, channel)
					st.RemoveSubscriber(channel, conn)

					resp := fmt.Sprintf(
						"*3\r\n$11\r\nunsubscribe\r\n$%d\r\n%s\r\n:%d\r\n",
						len(channel),
						channel,
						len(subscriptions),
					)
					conn.Write([]byte(resp))
				}
				// If none were subscribed, still send one response with empty channel.
				if len(subscriptions) == 0 {
					conn.Write([]byte("*3\r\n$11\r\nunsubscribe\r\n$-1\r\n:0\r\n"))
				}
				continue
			}

			for _, channel := range parts[1:] {
				_, wasSubscribed := subscriptions[channel]
				if wasSubscribed {
					delete(subscriptions, channel)
					st.RemoveSubscriber(channel, conn)
				}

				resp := fmt.Sprintf(
					"*3\r\n$11\r\nunsubscribe\r\n$%d\r\n%s\r\n:%d\r\n",
					len(channel),
					channel,
					len(subscriptions),
				)
				conn.Write([]byte(resp))
			}
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

func isAllowedInSubscribedMode(cmd string) bool {
	switch cmd {
	case "SUBSCRIBE", "UNSUBSCRIBE", "PSUBSCRIBE", "PUNSUBSCRIBE", "PING", "QUIT", "RESET":
		return true
	default:
		return false
	}
}
