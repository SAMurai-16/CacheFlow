package commands

import (
	"fmt"
	"net"
	"strconv"
	"time"

	"samyak.go_redis/store"
)

func HandleWAIT(st *store.Store, parts []string) []byte {
	if len(parts) != 3 {
		return []byte("-ERR wrong number of arguments for 'wait' command\r\n")
	}

	numReplicas, err := strconv.Atoi(parts[1])
	if err != nil || numReplicas < 0 {
		return []byte("-ERR value is not an integer or out of range\r\n")
	}

	timeoutMs, err := strconv.Atoi(parts[2])
	if err != nil || timeoutMs < 0 {
		return []byte("-ERR value is not an integer or out of range\r\n")
	}

	st.Mu.RLock()
	targetOffset := st.ReplOffset
	connectedReplicas := len(st.Replicas)
	replicas := append([]net.Conn(nil), st.Replicas...)
	pendingWrites := targetOffset > st.LastWaitOffset
	st.Mu.RUnlock()

	if targetOffset == 0 {
		return []byte(fmt.Sprintf(":%d\r\n", connectedReplicas))
	}

	ackedReplicas := countAckedReplicas(st, targetOffset)
	if ackedReplicas >= numReplicas {
		return []byte(fmt.Sprintf(":%d\r\n", ackedReplicas))
	}

	if pendingWrites && len(replicas) > 0 {
		requestReplicaAcks(replicas)

		st.Mu.Lock()
		if targetOffset > st.LastWaitOffset {
			st.LastWaitOffset = targetOffset
		}
		st.Mu.Unlock()
	}

	deadline := time.Now().Add(time.Duration(timeoutMs) * time.Millisecond)
	for time.Now().Before(deadline) {
		ackedReplicas = countAckedReplicas(st, targetOffset)
		if ackedReplicas >= numReplicas {
			return []byte(fmt.Sprintf(":%d\r\n", ackedReplicas))
		}

		time.Sleep(10 * time.Millisecond)
	}

	ackedReplicas = countAckedReplicas(st, targetOffset)
	return []byte(fmt.Sprintf(":%d\r\n", ackedReplicas))
}

func countAckedReplicas(st *store.Store, targetOffset int64) int {
	st.Mu.RLock()
	defer st.Mu.RUnlock()

	count := 0
	for _, ackOffset := range st.ReplicaAck {
		if ackOffset >= targetOffset {
			count++
		}
	}

	return count
}

func requestReplicaAcks(replicas []net.Conn) {
	getAck := []byte("*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n")

	for _, replica := range replicas {
		replica.Write(getAck)
	}
}
