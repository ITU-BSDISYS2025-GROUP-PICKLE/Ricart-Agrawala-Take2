package main

import (
	"context"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"google.golang.org/grpc"

	pb "module/proto"
)

// Node object
type Node struct {
	pb.UnimplementedNodeServer

	port  string // Serves as both the localhost port and ID
	peers []string

	mu      sync.Mutex
	state   string
	lamport int32

	queued_replies []string
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

// RPC function
func (n *Node) ReceiveCSRequest(_ context.Context, req *pb.CSRequest) (*pb.CSResponse, error) {
	// TODO: Implement
	return nil, nil
}

// Respond to a received CS request
func (n *Node) RespondToCSRequest(peer string) {
	// TODO: Implement
}

// Simulate entering the critical section
func (n *Node) EnterCS() {
	// TODO: Implement
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

	// Start server
	go n.StartServer()

	// TODO: Request CS a few times

	// Shut down after 2 minutes
	time.Sleep(2 * time.Minute)
}
