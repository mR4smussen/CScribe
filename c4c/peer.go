package main

/* TODO:
* Make sure the ring order is correct after a new peer connects
* Make sure the finger tables are updated when a new peer connects
 */

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"net"
	"os"
	"strings"
	"sync"
)

var HASH_SIZE = 256

// Struct for a peer in the network
type Peer struct {
	ID          string
	IP          string
	Port        string
	fingerTable []Finger
	successor   *Peer
	predecessor *Peer
	connMutex   sync.Mutex // mutex used for the finger table
}

type Finger struct {
	peer      Peer
	start     string
	interval  *Interval
	successor string
}

type Interval struct {
	iStart string
	iEnd   string
}

// Struct for a message
type Message struct {
	Type string
	Data []byte
}

// Initializes a new peer
func NewPeer(IP, Port string) *Peer {
	// Compute the unique ID for the IP address
	ID_hash := sha256.Sum256([]byte(IP + ":" + Port))
	ID_string := hex.EncodeToString(ID_hash[:])
	fmt.Println("id for port:" + Port + " = " + ID_string)
	peer := Peer{
		ID:          ID_string,
		IP:          IP,
		Port:        Port,
		fingerTable: []Finger{},
		successor:   &Peer{},
		predecessor: &Peer{},
	}
	fingerTable := peer.initializeFingerTable(string(ID_string))
	peer.fingerTable = fingerTable
	peer.successor = &peer
	peer.predecessor = &peer
	return &peer
}

// Make new finger table from id
func (thisPeer *Peer) initializeFingerTable(id string) []Finger {
	// id as decimal
	idDecimal := new(big.Int)
	idDecimal.SetString(id, 16)

	fingerTable := make([]Finger, HASH_SIZE)

	// Set start value for each finger
	for i := 0; i < HASH_SIZE; i++ {
		start := computeStart(idDecimal, i+1)

		fingerTable[i] = Finger{
			peer:      *thisPeer,
			start:     start,
			interval:  &Interval{iStart: "", iEnd: ""},
			successor: thisPeer.ID,
		}
	}

	// Set intervals for each finger [finger[i].start, finger[i+1].start)
	for i := 0; i < HASH_SIZE-1; i++ {
		fingerTable[i].interval.iStart = fingerTable[i].start
		decimalStart := new(big.Int)
		decimalStart.SetString(fingerTable[i+1].start, 16)
		fingerTable[i].interval.iEnd =
			fmt.Sprintf("%064x", new(big.Int).Sub(decimalStart, big.NewInt(1)))
	}
	// wrap around
	fingerTable[HASH_SIZE-1].interval.iStart = fingerTable[HASH_SIZE-1].start
	decimalStart := new(big.Int)
	decimalStart.SetString(fingerTable[0].start, 16)
	fingerTable[HASH_SIZE-1].interval.iEnd =
		fmt.Sprintf("%064x", new(big.Int).Sub(decimalStart, big.NewInt(1)))

	return fingerTable
}

// Helper function to compute decimal(id) + 2^(i-1)
func computeStart(decimalID *big.Int, i int) string {
	// Calculate 2^(i-1)
	intervalSize := new(big.Int).Exp(big.NewInt(2), big.NewInt(int64(i-1)), nil)

	// Add decimal(id) + 2^(i-1)
	result := new(big.Int).Add(decimalID, intervalSize)

	// Mod 2^256
	mod := new(big.Int).Exp(big.NewInt(2), big.NewInt(int64(HASH_SIZE)), nil)
	result.Mod(result, mod)

	return fmt.Sprintf("%064x", result)
}

// Listening for incoming messages
func (thisPeer *Peer) Listen() {
	address := thisPeer.IP + ":" + thisPeer.Port
	listener, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("Error starting listener: %v", err)
	}
	defer listener.Close()

	for {
		conn, _ := listener.Accept()
		encoder := json.NewEncoder(conn)
		decoder := json.NewDecoder(conn)
		var message Message
		err := decoder.Decode(&message)
		if err != nil {
			fmt.Printf("Got an error while decoding:%v\n", err)
			continue
		}
		thisPeer.handleMessage(encoder, &message)
	}
}

func (thisPeer *Peer) handleMessage(encoder *json.Encoder, message *Message) {
	switch message.Type {
	case "GetPeer":
		encoder.Encode(thisPeer)
	case "GetSuccessor":
		encoder.Encode(thisPeer.successor)
	case "GetFingertable":
		encoder.Encode(&thisPeer.fingerTable)
	case "GetClosestPrecedingFinger":
		var id string
		json.Unmarshal(message.Data, &id)
		closest := thisPeer.closest_preceding_finger(id)
		encoder.Encode(closest)
	}
}

// Connect to another peer
func (thisPeer *Peer) GetConnection(otherAddr string) net.Conn {
	conn, err := net.Dial("tcp", otherAddr)
	if err != nil {
		log.Printf("Error connecting to peer: %v", err)
		return nil
	}

	return conn
}

// Menu to display options and take input
func (p *Peer) Menu() {
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Println("\n###################################")
		fmt.Println("##### Welcome to Chord4Convos #####")
		fmt.Println("###################################")
		fmt.Println("Choose an option: ")
		fmt.Println("1. Print fingertable")
		fmt.Println("2. Temp option 2")
		fmt.Println("3. Exit")

		choice, _ := reader.ReadString('\n')
		choice = strings.TrimSpace(choice)

		switch choice {
		case "1":
			printFingertable(p.fingerTable)

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

// Find id's successor
func (thisPeer *Peer) find_successor(id string) *Peer {
	oldPredecessor := thisPeer.find_predecessor(id)
	// get successor from oldPredecessor
	oldPredecessorAddr := oldPredecessor.IP + ":" + oldPredecessor.Port
	res := &Peer{}
	thisPeer.sendMessage(oldPredecessorAddr, "GetSuccessor", nil, res)
	return res
}

// Find id's predecessor
func (thisPeer *Peer) find_predecessor(id string) *Peer {
	currentClosest := thisPeer

	currentClosestAddress := currentClosest.IP + ":" + currentClosest.Port
	currentClosestSuccessor := &Peer{}
	thisPeer.sendMessage(currentClosestAddress, "GetSuccessor", nil, currentClosestSuccessor)

	for !(id > currentClosest.ID && id <= currentClosestSuccessor.ID ||
		currentClosest.ID == currentClosestSuccessor.ID) {
		currentClosestAddress := currentClosest.IP + ":" + currentClosest.Port
		thisPeer.sendMessage(currentClosestAddress, "GetClosestPrecedingFinger", id, currentClosest)
		thisPeer.sendMessage(currentClosestAddress, "GetSuccessor", nil, currentClosestSuccessor)
	}
	return currentClosest
}

// Return closes preceding finger
func (thisPeer *Peer) closest_preceding_finger(id string) *Peer {
	thisPeer.connMutex.Lock()
	defer thisPeer.connMutex.Unlock()
	for i := HASH_SIZE - 1; i >= 0; i-- {
		fingerNode := thisPeer.fingerTable[i].peer
		if fingerNode.ID > thisPeer.ID &&
			fingerNode.ID < id {
			return &fingerNode
		}
	}
	return thisPeer
}

// `thisPeer` joins the network;
func (thisPeer *Peer) join(existingPeer *Peer) {
	if existingPeer != nil {
		thisPeer.init_finger_table(existingPeer)
		thisPeer.update_others()
		fmt.Println("Peer ", thisPeer.ID, "Successfully joined the network.")
	} else { // thisPeer is the only peer in the network
		thisPeer.connMutex.Lock()
		for i := 0; i < HASH_SIZE; i++ {
			thisPeer.fingerTable[i].peer = *thisPeer
		}
		thisPeer.predecessor = thisPeer
		thisPeer.connMutex.Unlock()
	}
}

// initialize finger table of local node thisPeer
func (thisPeer *Peer) init_finger_table(existingPeer *Peer) {
	thisPeer.connMutex.Lock()
	thisPeer.fingerTable[1].peer = *existingPeer.find_predecessor(thisPeer.fingerTable[0].start)
	thisPeer.predecessor = thisPeer.successor.predecessor
	thisPeer.successor.predecessor = thisPeer
	for i := 0; i < HASH_SIZE-1; i++ {
		nextFinger := thisPeer.fingerTable[i+1].start
		if nextFinger >= thisPeer.ID &&
			nextFinger < thisPeer.fingerTable[i].peer.ID {
			thisPeer.fingerTable[i+1].peer = thisPeer.fingerTable[i].peer
		} else {
			thisPeer.fingerTable[i+1].peer =
				*existingPeer.find_successor(thisPeer.fingerTable[i+1].start)
		}
	}
	thisPeer.connMutex.Unlock()
}

// update all nodes whose finger tables should refer to thisPeer
func (thisPeer *Peer) update_others() {
	thisPeer.connMutex.Lock()
	thisPeerDecimal := new(big.Int)
	thisPeerDecimal.SetString(thisPeer.ID, 16)

	for i := 0; i < HASH_SIZE; i++ {
		offsetI := new(big.Int).Exp(big.NewInt(2), big.NewInt(int64(i-1)), nil)
		nMinusOffset := new(big.Int).Sub(thisPeerDecimal, offsetI)
		p := thisPeer.find_predecessor(fmt.Sprintf("%064x", nMinusOffset))
		p.update_finger_table(thisPeer, i)
	}

	thisPeer.connMutex.Unlock()
}

// if s is ith finger of thisPeer, update thisPeers' finger table with s
func (thisPeer *Peer) update_finger_table(s *Peer, i int) {
	if s.ID >= thisPeer.ID &&
		s.ID < thisPeer.fingerTable[i].peer.ID {
		thisPeer.fingerTable[i].peer = *s
		p := thisPeer.predecessor
		p.update_finger_table(s, i)
	}
}

// Makes a new message with a given type
func newMessage(Type string) *Message {
	message := new(Message)
	message.Type = Type
	message.Data = []byte{}
	return message
}

// Sends a message to the given IP
func (thisPeer *Peer) sendMessage(ip string, msgType string, data interface{}, response interface{}) {
	conn := thisPeer.GetConnection(ip)
	if conn == nil {
		log.Printf("Was not able to connect to %s.", ip)
		return
	}
	defer conn.Close()

	encoder := json.NewEncoder(conn)
	decoder := json.NewDecoder(conn)

	message := newMessage(msgType)
	if data != nil {
		message.Data, _ = json.Marshal(data)
	}
	encoder.Encode(message)
	decoder.Decode(response)
}

func printFingertable(fingers []Finger) {
	fmt.Println("Printing fingertable...")
	for i, finger := range fingers {
		fmt.Println(i, "-", finger.peer.ID)
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

	peer := NewPeer("localhost", listenPort)
	go peer.Listen()

	if connectPort != "0" {
		connectAddr := "localhost:" + connectPort
		connectPeer := &Peer{}
		peer.sendMessage(connectAddr, "GetPeer", nil, connectPeer)

		// connect to the network through the connectPeer
		fmt.Println("Peer", peer.ID, "is joining the network through", connectPeer.ID)
		peer.join(connectPeer)
	} else {
		peer.join(nil)
	}
	peer.Menu()
}
