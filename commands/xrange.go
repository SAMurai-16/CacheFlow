package commands

import (
	"fmt"
	"strings"

	"samyak.go_redis/store"
)

func HandleXRANGE(st *store.Store, parts []string) []byte {
	if len(parts) < 4 {
		return []byte("-ERR wrong number of arguments\r\n")
	}

	key := parts[1]
	start := parts[2]
	end := parts[3]

	entries, err := st.XRange(key, start, end)
	if err != nil {
		return []byte("-ERR invalid range\r\n")
	}

	var resp strings.Builder

	resp.WriteString(fmt.Sprintf("*%d\r\n", len(entries)))

	for _, e := range entries {
		resp.WriteString("*2\r\n")

		// Entry ID
		resp.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(e.ID), e.ID))

		// Field-value array
		resp.WriteString(fmt.Sprintf("*%d\r\n", len(e.Fields)*2))

		for field, value := range e.Fields {
			resp.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(field), field))
			resp.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(value), value))
		}
	}

	return []byte(resp.String())
}
