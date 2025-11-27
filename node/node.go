package main

import (
	"context"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "module/proto"
)

// Node object
type Node struct {
	pb.UnimplementedNodeServer

	port  string   // Serves as both the localhost port and ID
	peers []string // Slice of peer-ports

	mu      sync.Mutex
	state   string // Must be among { "RELEASED", "WANTED", "HELD" }
	lamport int32  // Lamport timestamp
	replies int    // Number of received replies

	queued_replies []string // Slice of peer-ports to reply to after leaving CS
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

// Request CS access from all peers
func (n *Node) RequestCSFromPeers() {
	for _, peer := range n.peers {
		go n.RequestCSFromPeer(peer)
	}

	for n.replies < len(n.peers) {
		// Wait for replies from all peers...
	}

	n.EnterAndLeaveCS()
}

// Request CS access from a peer at a given address
func (n *Node) RequestCSFromPeer(peer_port string) {
	// Create address
	address := "localhost:" + peer_port

	// Create connection
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Connect to server failed: %v", err)
	}

	// Create intermediate peer client
	peer := pb.NewNodeClient(conn)

	// Create CS request
	portInt, err := strconv.Atoi(n.port)
	if err != nil {
		log.Fatalf("Node-port conversion failed: %v", err)
	}

	portInt32 := int32(portInt)

	req := &pb.CSRequest{
		NodePort: portInt32,
		Lamport:  n.lamport,
	}

	// Request CS from peer
	_, err = peer.ReceiveCSRequest(context.Background(), req)
	if err != nil {
		log.Fatalf("ReceiveCSRequest failed: %v", err)
	}

	// TODO: This functionality goes in RespondToCSRequest(...)
	// Increment the requesting node's amount of received replies
	// n.mu.Lock()
	// n.replies++
	// n.mu.Unlock()
}

// RPC function
func (n *Node) ReceiveCSRequest(_ context.Context, req *pb.CSRequest) (*pb.Empty, error) {
	// TODO: Implement
	return nil, nil
}

// Respond to a received CS request
func (n *Node) RespondToCSRequest(peer string) {
	// TODO: Implement
}

// Simulate entering the critical section
func (n *Node) EnterAndLeaveCS() {
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
	log.Println("Server shutting down...")
}
