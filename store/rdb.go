package store

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

func (s *Store) LoadRDB() error {
	path := filepath.Join(s.Dir, s.DBFilename)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	if len(data) < 9 || string(data[:9]) != "REDIS0011" {
		return fmt.Errorf("invalid rdb header")
	}

	i := 9
	var expireAt time.Time

	for i < len(data) {
		
		expireAt = time.Time{}
		op := readByte(data, &i)

		switch op {
			case 0xFA:
				//metadata 
				_ = readString(data,&i)
				_ = readString(data,&i)
			case 0xFE:
				//database selector
				_ = readLength(data,&i)
			case 0xFB:
				//hash table sizes
				_ = readLength(data,&i)
				_ = readLength(data,&i)
			case 0xFD:
				// expire in seconds
				expire := int(data[i]) |
					int(data[i+1])<<8 |
					int(data[i+2])<<16 |
					int(data[i+3])<<24

				i += 4

				expireAt = time.Unix(int64(expire), 0)

				valType := readByte(data, &i)

				if valType == 0 {
					key := readString(data, &i)
					val := readString(data, &i)

					s.Set(key, val, expireAt)
				}

			case 0xFC:
				// expire in milliseconds
				expire := int64(data[i]) |
					int64(data[i+1])<<8 |
					int64(data[i+2])<<16 |
					int64(data[i+3])<<24 |
					int64(data[i+4])<<32 |
					int64(data[i+5])<<40 |
					int64(data[i+6])<<48 |
					int64(data[i+7])<<56

				i += 8

				expireAt = time.UnixMilli(expire)

				valType := readByte(data, &i)

				if valType == 0 {
					key := readString(data, &i)
					val := readString(data, &i)

					s.Set(key, val, expireAt)
				}

			case 0x00:
				// string value
				key := readString(data, &i)
				val := readString(data, &i)

				s.Set(key, val, time.Time{})

			case 0xFF:
				// EOF
				return nil

			default:
				return fmt.Errorf("unknown opcode %x", op)
			}
	} 
	return nil
}




func readByte(data []byte, i *int) byte {
	b := data[*i]
	*i++
	return b
}


func readLength(data []byte, i *int) int {
	b := readByte(data, i)

	prefix := b >> 6

	switch prefix {

	case 0: // 00xxxxxx
		return int(b & 0x3F)

	case 1: // 01xxxxxx (14-bit)
		next := readByte(data, i)
		return int(b&0x3F)<<8 | int(next)

	case 2: // 32-bit
		val := int(data[*i])<<24 |
			int(data[*i+1])<<16 |
			int(data[*i+2])<<8 |
			int(data[*i+3])

		*i += 4
		return val

	default:
		panic("special encoding not supported")
	}
}


func readString(data []byte, i *int) string {
	length := readLength(data, i)

	str := string(data[*i : *i+length])
	*i += length

	return str
}