package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

type CommandFunc func(c net.Conn, args []string) string

type Record struct {
	Value     any
	CreatedAt time.Time
	ExpiresAt time.Time
}

var CommandMap = map[string]CommandFunc{
	"PING": PingCommand,
	"ECHO": EchoCommand,
	"SET":  SetCommand,
	"GET":  GetCommand,
}

var SetStore = map[string]*Record{}

// *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n -> ["ECHO", "hey"]
func parseCommand(reader *bufio.Reader) ([]string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		fmt.Println("Error in reading from reader")
		return nil, err
	}

	// trim the line string received
	line = strings.TrimSpace(line)
	fmt.Println("trimmed line input -> ", line)

	if line == "" {
		fmt.Println("command is empty")
		return nil, err
	}

	switch line[0] {
	case '+', '-', ':', '$':
		return []string{line}, nil
	case '*':
		return parseArray(reader, line)

	default:
		return nil, errors.New("invalid command Received")

	}

}

func parseArray(reader *bufio.Reader, line string) ([]string, error) {
	numArgs, err := strconv.Atoi(strings.TrimSpace(line[1:]))
	if err != nil {
		return nil, errors.New("invalid num of args received")
	}

	// looping numArgs int value
	args := make([]string, numArgs)
	for i := 0; i < numArgs; i++ {
		line, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}

		line = strings.TrimSpace(line)
		fmt.Println("inside trimmed line in parse array ", line)
		if line == "" {
			return nil, errors.New("inner empty line")
		}

		switch line[0] {
		case '$':
			length, err := strconv.Atoi(strings.TrimSpace(line[1:]))
			if err != nil {
				return nil, errors.New("invalid bulk string length")
			}

			// making buf byte input for the length mentioned above to read input
			arg := make([]byte, length)
			_, err = io.ReadFull(reader, arg)
			if err != nil {
				return nil, err
			}
			// reader to read trailing \r\n
			_, err = reader.ReadString('\n')
			if err != nil {
				return nil, err
			}

			args[i] = string(arg)
		default:
			return nil, errors.New("invalid bulk string format")
		}

	}
	return args, nil
}

func executeCommand(c net.Conn, args []string) string {
	if len(args) == 0 {
		return "-ERR no command provided"
	}

	fmt.Println("execute command received -> ", args)
	switch args[0][0] {
	case '+':
		return fmt.Sprintf("%s\r\n", args[0])

	case '-':
		return fmt.Sprintf("%s\r\n", args[0])

	case ':':
		return fmt.Sprintf("%s\r\n", args[0])

	case '$':
		length, err := strconv.Atoi(strings.TrimSpace(args[0][1:]))
		if err != nil {
			return "-ERR invalid bulk string length"
		}
		return fmt.Sprintf("%d\r\n%s\r\n", length, args[1])

	default:
		commands := strings.ToUpper(args[0])
		if commandFunc, exists := CommandMap[commands]; exists {
			return commandFunc(c, args[1:])
		}
		return "-ERR Unknown Command"
	}
}

func PingCommand(c net.Conn, args []string) string {
	if len(args) > 0 {
		return "-ERR wrong number of argument for 'PING' command\r\n"
	}
	return fmt.Sprintf("+PONG\r\n")
}

func EchoCommand(c net.Conn, args []string) string {
	if len(args) != 1 {
		return "-ERR wong number of arguments for 'ECHO' command\r\n"
	}
	return fmt.Sprintf("+%s\r\n", args[0])
}

func SetCommand(c net.Conn, args []string) string {
	if len(args) < 2 {
		return "-ERR wrong number of arguments for 'SET' commands\r\n"
	}
	key := args[0]
	val := args[1]

	record := &Record{
		Value:     val,
		CreatedAt: time.Now(),
		ExpiresAt: time.Time{},
	}

	// check if px arg provided
	if len(args) == 4 && strings.ToUpper(args[2]) == "PX" {
		expiration, err := strconv.Atoi(args[3])
		if err != nil {
			return "-ERR invalid expiration time provided for 'SET' command\r\n"
		}
		record.ExpiresAt = time.Now().Add(time.Duration(expiration) * time.Millisecond)

	}
	SetStore[key] = record
	return fmt.Sprintf("+OK\r\n")
}

func GetCommand(c net.Conn, args []string) string {
	if len(args) != 1 {
		return "-ERR wrong number of arguments for 'GET' commands"
	}

	key := args[0]
	val, prst := SetStore[key]
	if !prst {
		return "$-1\r\n"
	}

	if time.Now().After(val.ExpiresAt) && !val.ExpiresAt.IsZero() {
		delete(SetStore, key)
		return "$-1\r\n"
	}
	return fmt.Sprintf("$%d\r\n%s\r\n", len(val.Value.(string)), val.Value)
}
