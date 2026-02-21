package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strings"

	"samyak.go_redis/commands"
	"samyak.go_redis/resp"
	"samyak.go_redis/store"
)




func main() {
	port := "6380"

	role := "master"
	masterHost := ""
	masterPort := ""

	for i := 0; i < len(os.Args); i++ {
		if os.Args[i] == "--port" && i+1 < len(os.Args) {
			port = os.Args[i+1]
			
		}

		if os.Args[i] == "--replicaof" && i+2 < len(os.Args) {
					role = "slave"
					masterHost = os.Args[i+1]
					masterPort = os.Args[i+2]
		}


	}


	ln, err := net.Listen("tcp", ":"+port)

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Connected to the server on port", port)

	// Create a single store instance for all connections
	st := store.New(role, masterHost, masterPort, port)

	if st.Role == "slave" {
	go connectToMaster(st)
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

		if cmd == "EXEC" {
			fmt.Println(inTransaction)
			if !inTransaction {
				conn.Write([]byte("-ERR EXEC without MULTI\r\n"))
				continue
			}

			var response [][]byte

			for _, queued := range queuedCommands {
				resp := executeCommands(st, queued)
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

		if inTransaction {
			queuedCommands = append(queuedCommands, parts)
			conn.Write([]byte("+QUEUED\r\n"))
			continue
		}

		resp := executeCommands(st, parts)
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
		return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg))

	case "REPLCONF":
	return []byte("+OK\r\n")

	case "PSYNC":
	return []byte(fmt.Sprintf(
		"+FULLRESYNC %s %d\r\n",
		st.ReplID,
		st.ReplOffset,
	))

	case "SET":
		return commands.HandleSET(st, parts)

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

	default:
		return ([]byte("-ERR unknown command\r\n"))

	}

}


func connectToMaster(st *store.Store) {

	conn, err := net.Dial("tcp", st.MasterHost+":"+st.MasterPort)
	if err != nil {
		fmt.Println("Failed to connect to master:", err)
		return
	}

	fmt.Println("Connected to master", st.MasterHost, st.MasterPort)

	reader := bufio.NewReader(conn)


	// 1) PING
	
	ping := "*1\r\n$4\r\nPING\r\n"
	conn.Write([]byte(ping))

	resp, _ := reader.ReadString('\n')
	fmt.Print("PING response:", resp)


	// 2) REPLCONF listening-port <PORT>
	
	replconf1 := fmt.Sprintf(
		"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$%d\r\n%s\r\n",
		len(st.ReplicaPort),
		st.ReplicaPort,
	)

	conn.Write([]byte(replconf1))

	resp, _ = reader.ReadString('\n')
	fmt.Print("REPLCONF1 response:", resp)

	// 3) REPLCONF capa psync2
	replconf2 :=
		"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"

	conn.Write([]byte(replconf2))

	resp, _ = reader.ReadString('\n')
	fmt.Print("REPLCONF2 response:", resp)

	// 4) PSYNC ? -1

	psync := 
		"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"
	conn.Write([]byte(psync))

	reader.ReadString('\n')


}
