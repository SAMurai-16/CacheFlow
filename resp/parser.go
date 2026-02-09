package resp

import (
	"bufio"
	"fmt"
	"strconv"
	"strings"
)

func ReadRESPArray(reader *bufio.Reader) ([]string, error) {

	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}

	line = strings.TrimSpace(line)
	if !strings.HasPrefix(line, "*") {
		return nil, fmt.Errorf("expected array")
	}

	count, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, err
	}

	result := make([]string,0, count)

	for i := 0; i < count; i++{

		lenLine, err := reader.ReadString('\n')
		if err != nil {
			return nil,err
		}

		lenLine = strings.TrimSpace(lenLine)
		if !strings.HasPrefix(lenLine,"$"){
			return nil, fmt.Errorf("expected bulk string")
		}

		strLen, err := strconv.Atoi(lenLine[1:])
		if err !=nil{
			return nil, err
		}

		data := make([]byte,strLen+2)
		_,err = reader.Read(data)
		if err!= nil {
			return nil,err
		}

		result = append(result,string(data[:strLen]))


	}

	return result,nil
}
