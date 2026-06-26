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
	if _, err := conn.Write([]byte("*1\r\n$4\r\nPING\r\n")); err != nil {
		fmt.Println("Failed to send PING to master:", err)
		return
	}
	if err := readExpectedLine(reader, "+PONG"); err != nil {
		fmt.Println("Invalid PING response from master:", err)
		return
	}

	// 2) REPLCONF listening-port
	replconf1 := fmt.Sprintf(
		"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$%d\r\n%s\r\n",
		len(st.ReplicaPort),
		st.ReplicaPort,
	)
	if _, err := conn.Write([]byte(replconf1)); err != nil {
		fmt.Println("Failed to send REPLCONF listening-port to master:", err)
		return
	}
	if err := readExpectedLine(reader, "+OK"); err != nil {
		fmt.Println("Invalid REPLCONF listening-port response from master:", err)
		return
	}

	// 3) REPLCONF capa psync2
	if _, err := conn.Write([]byte(
		"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n",
	)); err != nil {
		fmt.Println("Failed to send REPLCONF capa to master:", err)
		return
	}
	if err := readExpectedLine(reader, "+OK"); err != nil {
		fmt.Println("Invalid REPLCONF capa response from master:", err)
		return
	}

	// 4) PSYNC ? -1
	if _, err := conn.Write([]byte(
		"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n",
	)); err != nil {
		fmt.Println("Failed to send PSYNC to master:", err)
		return
	}

	// FULLRESYNC line
	fullResync, err := readLine(reader)
	if err != nil {
		fmt.Println("Failed to read FULLRESYNC response from master:", err)
		return
	}
	if !strings.HasPrefix(fullResync, "+FULLRESYNC ") {
		fmt.Println("Invalid PSYNC response from master:", fullResync)
		return
	}

	// READ RDB BULK HEADER
	header, err := readLine(reader) // e.g. "$88"
	if err != nil {
		fmt.Println("Failed to read RDB header from master:", err)
		return
	}

	if len(header) == 0 || header[0] != '$' {
		fmt.Println("Invalid RDB header:", header)
		return
	}

	lengthStr := header[1:]
	length, err := strconv.Atoi(lengthStr)
	if err != nil {
		fmt.Println("Invalid RDB length:", lengthStr)
		return
	}
	if length < 0 {
		fmt.Println("Invalid RDB length:", length)
		return
	}

	// READ EXACT BINARY DATA
	rdb := make([]byte, length)
	if _, err := io.ReadFull(reader, rdb); err != nil {
		fmt.Println("Failed to read RDB snapshot from master:", err)
		return
	}

	fmt.Println("Received RDB snapshot:", length, "bytes")

	// NOW start replication stream(salve reads)
	for {
		parts, err := resp.ReadRESPArray(reader)
		if err != nil {
			fmt.Println("Replication stream closed:", err)
			return
		}

		if len(parts) == 0 {
			continue
		}

		cmd := strings.ToUpper(parts[0])
		size := respArraySize(parts)

		if cmd == "REPLCONF" && len(parts) >= 2 &&
			strings.ToUpper(parts[1]) == "GETACK" {

			
			st.ReplOffset += size
			sendACK(conn, st.ReplOffset)
			continue
		}

		commands.ApplyReplicaCommand(st, parts)

		st.ReplOffset += size
	}
}

func readExpectedLine(reader *bufio.Reader, expected string) error {
	line, err := readLine(reader)
	if err != nil {
		return err
	}

	if line != expected {
		return fmt.Errorf("expected %q, got %q", expected, line)
	}

	return nil
}

func readLine(reader *bufio.Reader) (string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}

	return strings.TrimSpace(line), nil
}

//for master 
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
