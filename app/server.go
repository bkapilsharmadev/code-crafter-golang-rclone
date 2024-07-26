package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
)

var (
	port        = flag.String("port", "6379", "port number to connect on")
	addr        = flag.String("addr", "0.0.0.0", "address of server")
	master_host = flag.String("master_host", "0.0.0.0", "master host ip addr")
	master_port = flag.String("master_port", "6379", "master port of server")
	role        = "master"
	replicaof   = flag.String("replicaof", "master", "role type of server, if host it will be master else slave")
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")
	flag.Parse()

	l, err := net.Listen("tcp", fmt.Sprintf("%s:%s", *addr, *port))
	if err != nil {
		fmt.Sprintf("Failed to binf port %s \n", *port)
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
