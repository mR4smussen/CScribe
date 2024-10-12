package main

/* TODO:
* Make sure the ring order is correct after a new peer connects
* Make sure the finger tables are updated when a new peer connects
 */

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"
)

// Struct for a peer in the network
type Peer struct {
	address     string // ip address of this peer
	fingerTable map[string]net.Conn
	connMutex   sync.Mutex // mutex used for the finger table
}

// Initializes a new peer
func NewPeer(address string) *Peer {
	return &Peer{
		address:     address,
		fingerTable: make(map[string]net.Conn),
	}
}

// Listening for incoming connections
func (thisPeer *Peer) Listen() {
	listener, err := net.Listen("tcp", thisPeer.address)
	if err != nil {
		log.Fatalf("Error starting listener: %v", err)
	}
	defer listener.Close()

	log.Printf("Listening on %s", thisPeer.address)
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %v", err)
			continue
		}
		go thisPeer.handleConnection(conn)
	}
}

// Sub-routing for new connections
func (thisPeer *Peer) handleConnection(conn net.Conn) {
	otherAddr := conn.RemoteAddr().String()
	thisPeer.connMutex.Lock()
	thisPeer.fingerTable[otherAddr] = conn
	thisPeer.connMutex.Unlock()

	// Remove the connection when it closes
	defer func() {
		thisPeer.connMutex.Lock()
		delete(thisPeer.fingerTable, otherAddr)
		thisPeer.connMutex.Unlock()
		conn.Close()
	}()

	// Handle message from the new connection
	log.Printf("Connected to %s", otherAddr)
	reader := bufio.NewReader(conn)
	for {
		message, err := reader.ReadString('\n')
		if err != nil {
			log.Printf("Connection closed by %s", otherAddr)
			return
		}
		message = strings.TrimSpace(message)
		log.Printf("Message received from %s: %s", otherAddr, message)
	}
}

// Connect to another peer
func (thisPeer *Peer) Connect(otherAddr string) {
	thisPeer.connMutex.Lock()
	_, exists := thisPeer.fingerTable[otherAddr]
	thisPeer.connMutex.Unlock()

	if exists {
		log.Printf("Already connected to peer: %s", otherAddr)
		return
	}

	conn, err := net.Dial("tcp", otherAddr)
	if err != nil {
		log.Printf("Error connecting to peer: %v", err)
		return
	}

	thisPeer.connMutex.Lock()
	thisPeer.fingerTable[otherAddr] = conn
	thisPeer.connMutex.Unlock()

	log.Printf("Connected to peer %s", otherAddr)

	go thisPeer.handleConnection(conn)
}

// Menu to display options and take input
func (p *Peer) Menu() {
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Println("\n###################################")
		fmt.Println("##### Welcome to Chord4Convos #####")
		fmt.Println("###################################")
		fmt.Println("Choose an option: ")
		fmt.Println("1. Temp option 1")
		fmt.Println("2. Temp option 2")
		fmt.Println("3. Exit")

		choice, _ := reader.ReadString('\n')
		choice = strings.TrimSpace(choice)

		switch choice {
		case "1":
			fmt.Println("Option 1 selected")

		case "2":
			fmt.Println("Option 2 selected")

		case "3":
			fmt.Println("Exiting...")
			os.Exit(0)
		default:
			fmt.Println("Invalid option, please try again.")
		}
	}
}

func main() {
	if len(os.Args) != 3 {
		fmt.Println("Usage: go run peer.go [listen_port] [connect_port]")
		fmt.Println("To create a new network, use \"0\" as the [connect_port]")
		os.Exit(1)
	}

	listenPort := os.Args[1]
	connectPort := os.Args[2]

	peer := NewPeer("localhost:" + listenPort)

	go peer.Listen()

	if connectPort != "0" {
		peer.Connect("localhost:" + connectPort)
	}

	peer.Menu()
}
