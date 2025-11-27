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

	port  int32    // Serves as both the localhost port and ID
	peers []string // Slice of peer-ports

	mu      sync.Mutex
	state   string // Must be among { "RELEASED", "WANTED", "HELD" }
	lamport int32  // Lamport timestamp
	replies int    // Number of received replies

	queued_replies []string // Slice of peer addresses to reply to after leaving CS
}

// Start a node server. Runs indefinitely
func (n *Node) StartServer() {
	// Convert node's port (back to) a string
	nPort := strconv.Itoa(int(n.port))

	// Create address
	address := "localhost:" + nPort

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

// Request CS access from all peers. Lecture 7 slide 15/50 'on enter do' part
func (n *Node) RequestCSFromPeers() {
	// Change state and increase lamport timestamp
	n.mu.Lock()
	n.state = "WANTED"
	n.lamport++
	log.Printf("Node #%d [T:%d] is requesting access to the critical section", n.port, n.lamport)
	n.mu.Unlock()

	// Start a goroutine requesting CS from every peer
	for _, peer := range n.peers {
		go n.RequestCSFromPeer(peer)
	}

	// Wait for replies from all peers...
	for n.replies < len(n.peers) {
		//time.Sleep(time.Second) // Check once per second instead of thousands of times
	}

	//Change state
	n.mu.Lock()
	n.state = "HELD"
	n.mu.Unlock()

	// Enter and eventually leave the critical section
	n.EnterAndLeaveCS()
}

// Request CS access from a peer at a given address
func (n *Node) RequestCSFromPeer(peer_port string) {
	// Create address
	address := "localhost:" + peer_port

	// Create connection
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Connecting failed: %v", err)
	}

	// Create intermediate peer client
	peer := pb.NewNodeClient(conn)

	// Create CS request
	req := &pb.CSRequest{
		NodePort: n.port,
		Lamport:  n.lamport,
	}

	// Request CS from peer
	_, err = peer.ReceiveCSRequest(context.Background(), req)
	if err != nil {
		log.Fatalf("ReceiveCSRequest failed: %v", err)
	}
}

// RPC function
func (n *Node) ReceiveCSRequest(_ context.Context, req *pb.CSRequest) (*pb.Empty, error) {
	// Create address
	reqPort := strconv.Itoa(int(req.NodePort))
	address := "localhost:" + reqPort

	// Lecture 7 slide 15/50 'on receive do' part
	if (n.state == "HELD") || (n.state == "WANTED" && n.IsLessThanPeer(req)) {
		// Queue reply
		n.queued_replies = append(n.queued_replies, address)
		log.Printf("Node #%d [T:%d] queued responding to Node #%s [T:%d]", n.port, n.lamport, reqPort, req.Lamport)
	} else {
		// Respond directly
		n.RespondToCSRequest(address)
		log.Printf("Node #%d [T:%d] responded directly to Node #%s [T:%d]", n.port, n.lamport, reqPort, req.Lamport)
	}

	// Upon receiving a CS request, update Lamport timestamp
	n.mu.Lock()
	n.lamport = max(n.lamport, req.Lamport) + 1
	n.mu.Unlock()

	return &pb.Empty{}, nil
}

// Return whether a node is 'less than' another.
// Primarily determined by Lamport timestamp, otherwise port-number
func (n *Node) IsLessThanPeer(peerReq *pb.CSRequest) bool {
	if n.lamport == peerReq.Lamport {
		return n.port < peerReq.NodePort
	}

	return n.lamport < peerReq.Lamport
}

// Respond to a received CS request
func (n *Node) RespondToCSRequest(address string) {
	// Create connection
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Connect to server failed: %v", err)
	}

	// Create intermediate peer client
	peer := pb.NewNodeClient(conn)

	// Create CS response
	resp := &pb.CSResponse{
		NodePort: n.port,
		Lamport:  n.lamport,
	}

	// Increment the node-we're-responding-to's number of received replies
	peer.IncrementReceived(context.Background(), resp)
}

// RPC function
func (n *Node) IncrementReceived(_ context.Context, resp *pb.CSResponse) (*pb.Empty, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.replies++
	log.Printf("Node #%d [T:%d] received a reply from Node #%d [T:%d]", n.port, n.lamport, resp.NodePort, resp.Lamport)

	return &pb.Empty{}, nil
}

// Simulate entering the critical section
func (n *Node) EnterAndLeaveCS() {
	// Enter and do something cool in the critical section
	log.Printf("Node #%d [T:%d] entered the critical section 🚨🐔", n.port, n.lamport)
	time.Sleep(3 * time.Second)
	log.Printf("Node #%d [T:%d] left the critical section 😴💭🐑", n.port, n.lamport)

	// Lecture 7 slide 15/50 'on exit do' part
	n.mu.Lock()
	n.state = "RELEASED"
	n.mu.Unlock()
	for _, address := range n.queued_replies {
		n.RespondToCSRequest(address)
	}

	// Reset queued replies and number of received replies
	n.mu.Lock()
	n.queued_replies = []string{}
	n.replies = 0
	n.mu.Unlock()
}

func main() {
	if len(os.Args) < 3 {
		// Even though we're really looking for 2 (or more) arguments, we expect 3 (or more)
		// because the first (0th) argument always refers to the .exe of the .go file that's run
		println("Usage: go run node.go <port_number> [<peer_port_1> ...]")
		return
	}

	// Parse port argument
	nPort, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatalf("Port-parsing failed: %v", err)
	}
	nPort32 := int32(nPort)

	// Parse peer argument(s)
	nPeers := os.Args[2:]

	// Create node. Lecture 7 slide 15/50 'on initialisation do' part
	n := &Node{
		port:    nPort32,
		peers:   nPeers,
		state:   "RELEASED",
		lamport: 0,
	}

	// Start server
	go n.StartServer()

	// Give time for the server to start
	time.Sleep(3 * time.Second)

	// Request CS a few times
	for range 5 {
		n.RequestCSFromPeers()
	}

	// Shut down server once every node has entered the CS a few times
	log.Println("Server shutting down...")
}
