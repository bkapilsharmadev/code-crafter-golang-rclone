package main

import (
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
			os.Exit(1)
		}

		go handleConn(c)
	}

	// *1\r\n$4\r\nPING\r\n
	//c.Write([]byte("+PONG\r\n"))
}

func handleConn(conn net.Conn) {
	defer conn.Close()

	for {
		buf := make([]byte, 1024)

		_, err := conn.Read(buf)
		if errors.Is(err, io.EOF) {
			fmt.Println("client closed the connections ", conn.RemoteAddr())
			break
		} else if err != nil {
			fmt.Println("Error while reading the message")
		}
		conn.Write([]byte("+PONG\r\n"))
		//break

	}

}
