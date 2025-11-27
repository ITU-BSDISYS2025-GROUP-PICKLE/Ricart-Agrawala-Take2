package main

import (
	"log"
	pb "module/proto"
	"net"
	"os"

	"google.golang.org/grpc"
)

// Node object
type Node struct {
	pb.UnimplementedNodeServer

	port    string
	peers   []string
	state   string
	lamport int32
}

// Start a node server. Runs indefinitely
func (n *Node) StartServer() {
	// Create address
	address := "localhost:" + n.port

	// Create listener
	lis, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("Listen failed: %v", err)
	}

	// Create and register server
	server := grpc.NewServer()
	pb.RegisterNodeServer(server, n)

	// Log for transparency
	log.Printf("Node server now listening on %s", address)

	// Serve
	if err := server.Serve(lis); err != nil {
		log.Fatalf("Serve failed: %v", err)
	}
}

func main() {
	if len(os.Args) < 3 {
		// Even though we're really looking for 2 (or more) arguments, we expect 3 (or more)
		// because the first (0th) argument always refers to the .exe of the .go file that's run
		println("Usage: go run node.go <port_number> [<peer_port_1> ...]")
		return
	}

	// Parse arguments
	nPort := os.Args[1]
	nPeers := os.Args[2:]

	// Create node
	n := &Node{
		port:    nPort,
		peers:   nPeers,
		state:   "RELEASED",
		lamport: 0,
	}

	n.StartServer()
}
