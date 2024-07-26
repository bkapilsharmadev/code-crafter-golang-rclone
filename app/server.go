package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to binf port 6379")
		os.Exit(1)
	}
	for {
		c, err := l.Accept()
		if err != nil {
			fmt.Println("Error accpeting new connections ", err.Error())
			break
		}

		go handleConn(c)
	}

	// *1\r\n$4\r\nPING\r\n
	//c.Write([]byte("+PONG\r\n"))
}

func handleConn(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	for {
		//buf := make([]byte, 1024)
		inputs, err := parseCommand(reader)

		if err != nil {
			if errors.Is(err, io.EOF) {
				fmt.Println("client closed the connections ", conn.RemoteAddr())

			} else if err != nil {
				fmt.Println("Error while reading the message")
				conn.Write([]byte("-ERR invalid command \r\n"))
			}
			break
		}

		//conn.Write([]byte("+PONG\r\n"))
		fmt.Println("parsed command which is recevied from client -< ", inputs)
		response := executeCommand(conn, inputs)
		// return the response
		_, err = conn.Write([]byte(response))
		if err != nil {
			fmt.Println("Error sending response to client ", err)
			break
		}
	}

}
