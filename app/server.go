package main

import (
	"bufio"
	"fmt"
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

		handleConn(c)
	}

	// *1\r\n$4\r\nPING\r\n
	//c.Write([]byte("+PONG\r\n"))
}

func handleConn(conn net.Conn) {
	defer conn.Close()
	commandReader := bufio.NewReader(conn)
	for {
		// inputs, err := commandReader.ReadString('\n')
		// if err != nil {
		// 	fmt.Println("Error parsing command")
		// 	conn.Write([]byte("-ERR invalid commands\r\n"))
		// }
		fmt.Println("inputs -> ", commandReader)
		//conn.Write([]byte("+PONG\r\n"))
	}

}
