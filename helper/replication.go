package helper

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"

	"samyak.go_redis/commands"
	"samyak.go_redis/resp"
	"samyak.go_redis/store"
)

func ConnectToMaster(st *store.Store) {

	conn, err := net.Dial("tcp", st.MasterHost+":"+st.MasterPort)
	if err != nil {
		fmt.Println("Failed to connect to master:", err)
		return
	}

	fmt.Println("Connected to master", st.MasterHost, st.MasterPort)

	reader := bufio.NewReader(conn)

	// 1) PING
	conn.Write([]byte("*1\r\n$4\r\nPING\r\n"))
	reader.ReadString('\n')

	// 2) REPLCONF listening-port
	replconf1 := fmt.Sprintf(
		"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$%d\r\n%s\r\n",
		len(st.ReplicaPort),
		st.ReplicaPort,
	)
	conn.Write([]byte(replconf1))
	reader.ReadString('\n')

	// 3) REPLCONF capa psync2
	conn.Write([]byte(
		"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n",
	))
	reader.ReadString('\n')

	// 4) PSYNC ? -1
	conn.Write([]byte(
		"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n",
	))

	// FULLRESYNC line
	reader.ReadString('\n')

	// READ RDB BULK HEADER
	header, _ := reader.ReadString('\n') // e.g. "$88\r\n"

	if len(header) == 0 || header[0] != '$' {
		fmt.Println("Invalid RDB header:", header)
		return
	}

	lengthStr := strings.TrimSpace(header[1:])
	length, _ := strconv.Atoi(lengthStr)

	// READ EXACT BINARY DATA
	rdb := make([]byte, length)
	io.ReadFull(reader, rdb)

	fmt.Println("Received RDB snapshot:", length, "bytes")

	// NOW start replication stream
	for {
		parts, err := resp.ReadRESPArray(reader)
		if err != nil {
			fmt.Println("Replication stream closed:", err)
			return
		}

		if len(parts)== 0 {
			continue
		}

		cmd := strings.ToUpper(parts[0])
		size := respArraySize(parts)

		if cmd == "REPLCONF" && len(parts) >= 2 &&
			strings.ToUpper(parts[1]) == "GETACK" {

				sendACK(conn, st.ReplOffset)

				st.ReplOffset += size
				continue
			}


		commands.ApplyReplicaCommand(st, parts)

		st.ReplOffset += size
	}
}

func PropagateToReplicas(st *store.Store, parts []string) {

	// Convert command to RESP array
	var builder strings.Builder

	builder.WriteString(fmt.Sprintf("*%d\r\n", len(parts)))

	for _, p := range parts {
		builder.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(p), p))
	}

	data := builder.String()

	st.Mu.Lock()
	replicas := append([]net.Conn(nil), st.Replicas...)
	st.ReplOffset += int64(len(data))
	st.Mu.Unlock()

	for _, r := range replicas {
		r.Write([]byte(data))
	}
}



func sendACK(conn net.Conn, offset int64) {
	offsetStr := strconv.FormatInt(offset, 10)

	resp := fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$%d\r\n%s\r\n", len(offsetStr), offsetStr)

	conn.Write([]byte(resp))
}


func respArraySize(parts []string) int64 {
	size := int64(0)

	size += int64(len(fmt.Sprintf("*%d\r\n", len(parts))))

	for _, p := range parts {
		size += int64(len(fmt.Sprintf("$%d\r\n%s\r\n", len(p), p)))
	}

	return size
}