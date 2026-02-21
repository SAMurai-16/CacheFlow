package commands

import (
	"fmt"

	"samyak.go_redis/store"
)

func HandleINFO(st *store.Store,parts []string) []byte {

	var info string

	if st.Role == "master" {

		info = fmt.Sprintf(
			"# Replication\r\nrole:master\r\nmaster_replid:%s\r\nmaster_repl_offset:%d\r\n",
			st.ReplID,
			st.ReplOffset,
		)

	} else {

		info = "# Replication\r\nrole:slave\r\n"
	}

	return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(info), info))
}
