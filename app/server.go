package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

// Server struct to hold server properties
type Server struct {
	Role                       string
	MasterAddress              string
	ConnectedSlaves            int
	MasterReplid               string
	MasterReplOffset           int64
	ReplBacklogActive          int
	ReplBacklogSize            int
	ReplBacklogFirstByteOffset int64
	ReplBacklogHistlen         int64
	Listener                   net.Listener
	DataStore                  map[string]*Record
	StoreMutex                 sync.RWMutex
	Stats                      ServerStats
	mutex                      sync.RWMutex
}

// ServerStats struct to hold server statistics
type ServerStats struct {
	Connections       int
	CommandsProcessed int
}

// Record type for holding record
type Record struct {
	Value     any
	CreatedAt time.Time
	ExpiresAt time.Time
}

var (
	port        = flag.String("port", "6379", "port number to connect on")
	addr        = flag.String("addr", "0.0.0.0", "address of server")
	master_host = flag.String("master_host", "0.0.0.0", "master host ip addr")
	master_port = flag.String("master_port", "6379", "master port of server")
	role        = "master"
	replicaof   = flag.String("replicaof", "", "role type of server, if host it will be master else slave")
)

func NewServer() *Server {
	return &Server{
		Role:                       "master",
		ConnectedSlaves:            0,
		MasterReplid:               "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
		MasterReplOffset:           0,
		ReplBacklogActive:          0,
		ReplBacklogSize:            1048576,
		ReplBacklogFirstByteOffset: 0,
		ReplBacklogHistlen:         0,
		DataStore:                  make(map[string]*Record),
		Stats:                      ServerStats{},
	}
}

func (s *Server) start(address string) error {
	var err error
	fmt.Println("address start -> ", address)
	s.Listener, err = net.Listen("tcp", address)
	if err != nil {
		fmt.Sprintf("failed to listen & bind for address %s \r\n", address)

	}
	fmt.Println("Rclone server is running at address -> ", address)

	for {
		c, err := s.Listener.Accept()
		if err != nil {
			fmt.Sprint("Error accepting new connections\r\n")
			continue
		}

		go s.handleConn(c)
	}
}

func (s *Server) handleConn(c net.Conn) {
	defer c.Close()

	reader := bufio.NewReader(c)
	for {
		command, err := parseCommand(reader)
		if err != nil {
			if err == io.EOF {
				fmt.Println("CLient closed connection")

			} else {
				c.Write([]byte("-ERR invalid Command\r\n"))
			}
			break
		}
		response := executeCommand(s, c, command)
		_, err = c.Write([]byte(response))
		if err != nil {
			fmt.Println("Error sending response to clients")
			break
		}
	}

}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")
	flag.Parse()

	// *1\r\n$4\r\nPING\r\n
	//c.Write([]byte("+PONG\r\n"))
	server := NewServer()
	if *replicaof != "" {
		server.Role = "slave"
		fmt.Println("Started server in slave mode")
		masterAddrPart := strings.Split(*replicaof, " ")
		if len(masterAddrPart) != 2 {
			fmt.Println("Invalid replicaof address format. Use <MASTER_HOST> <MASTER_PORT>")
			os.Exit(1)
		}
		server.MasterAddress = masterAddrPart[0] + ":" + masterAddrPart[1]
		fmt.Printf("starting slave server in replica mode of master : %s\n", *replicaof)

		// initaiate handeshake with master server
		if err := server.InitiateReplicationHandshake(); err != nil {
			fmt.Printf("137 Failed to initiate handshake : %s\n", err)
			os.Exit(1)
		}
	} else {
		server.Role = "master"
		fmt.Println("Started server in master mode")
	}

	if err := server.start(fmt.Sprintf("%s:%s", *addr, *port)); err != nil {
		fmt.Println("Error starting server ", err)
		os.Exit(1)
	}

}

// send command to master server and returns the response
func (s *Server) SendCommandToMaster(command []string) (string, error) {
	conn, err := net.Dial("tcp", s.MasterAddress)
	if err != nil {
		return "", fmt.Errorf("failed to connect to master %w", err)
	}
	defer conn.Close()

	//use the existing command execution pattern to handle the response
	commandString := fmt.Sprintf("*%d\r\n", len(command))
	for _, arg := range command {
		commandString += fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg)
	}

	_, err = conn.Write([]byte(commandString))
	if err != nil {
		return "", fmt.Errorf("failed to send command to master -> %w", err)
	}

	reader := bufio.NewReader(conn)
	response, err := reader.ReadString('\n')
	if err != nil {
		return "", fmt.Errorf("failed to read response from master: %w", err)
	}

	return strings.TrimSpace(response), nil
}

// initial replication handshake with the master server
func (s *Server) InitiateReplicationHandshake() error {
	pingRes, err := s.SendCommandToMaster([]string{"PING"})
	if err != nil {
		return fmt.Errorf("handshake failed : %w", err)
	}

	// ping response
	if pingRes != "+PONG" {
		return fmt.Errorf("handshake failed: expected +PONG, got %s ", pingRes)
	}

	fmt.Println("Handshake successful: received pong from master")
	return nil
}

// func handleConn(conn net.Conn) {
// 	defer conn.Close()
// 	reader := bufio.NewReader(conn)

// 	for {
// 		//buf := make([]byte, 1024)
// 		inputs, err := parseCommand(reader)

// 		if err != nil {
// 			if errors.Is(err, io.EOF) {
// 				fmt.Println("client closed the connections ", conn.RemoteAddr())

// 			} else if err != nil {
// 				fmt.Println("Error while reading the message")
// 				conn.Write([]byte("-ERR invalid command \r\n"))
// 			}
// 			break
// 		}

// 		//conn.Write([]byte("+PONG\r\n"))
// 		fmt.Println("parsed command which is recevied from client -< ", inputs)
// 		response := executeCommand(conn, inputs)
// 		// return the response
// 		_, err = conn.Write([]byte(response))
// 		if err != nil {
// 			fmt.Println("Error sending response to client ", err)
// 			break
// 		}
// 	}

// }
