package main

/* TODO:
* Make sure the ring order is correct after a new peer connects
* Make sure the finger tables are updated when a new peer connects
 */

import (
	"bufio"
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"net"
	"os"
	"strings"
	"sync"
)

var HASH_SIZE = 160

// Struct for a peer in the network
type Peer struct {
	ID          big.Int
	IP          string
	Port        string
	fingerTable []Finger
	successor   *Peer
	predecessor *Peer
	connMutex   sync.Mutex // mutex used for the finger table
}

type Finger struct {
	peer      Peer
	start     big.Int
	interval  *Interval
	successor big.Int
}

type Interval struct {
	iStart big.Int
	iEnd   big.Int
}

// Struct for a message
type Message struct {
	Type string
	Data []byte
}

type UpdateFingertableRpc struct {
	Peer  Peer
	Index int
}

// Initializes a new peer
func NewPeer(IP, Port string) *Peer {
	// Compute the unique ID for the IP address
	hash := sha1.New()
	hash.Write([]byte(IP + ":" + Port))
	hashBytes := hash.Sum(nil)
	ID := new(big.Int).SetBytes(hashBytes[:])
	peer := Peer{
		ID:          *ID,
		IP:          IP,
		Port:        Port,
		fingerTable: []Finger{},
		successor:   &Peer{},
		predecessor: &Peer{},
	}
	fingerTable := peer.initializeFingerTable(ID)
	peer.fingerTable = fingerTable
	peer.successor = &peer
	peer.predecessor = &peer
	return &peer
}

// Make new finger table from id
func (thisPeer *Peer) initializeFingerTable(id *big.Int) []Finger {
	fingerTable := make([]Finger, HASH_SIZE)

	// Set start value for each finger
	for i := 0; i < HASH_SIZE; i++ {
		start := computeStart(id, i+1)
		fingerTable[i] = Finger{
			peer:      *thisPeer,
			start:     *start,
			interval:  &Interval{iStart: *new(big.Int), iEnd: *new(big.Int)},
			successor: thisPeer.ID,
		}
	}

	// Set intervals for each finger [finger[i].start, finger[i+1].start)
	for i := 0; i < HASH_SIZE-1; i++ {
		fingerTable[i].interval.iStart = fingerTable[i].start
		fingerTable[i].interval.iEnd = *new(big.Int).Sub(&fingerTable[i+1].start, big.NewInt(1))
	}
	// wrap around
	fingerTable[HASH_SIZE-1].interval.iStart = fingerTable[HASH_SIZE-1].start
	fingerTable[HASH_SIZE-1].interval.iEnd = *new(big.Int).Sub(&fingerTable[0].start, big.NewInt(1))

	return fingerTable
}

// Helper function to compute decimal(id) + 2^(i-1)
func computeStart(decimalID *big.Int, i int) *big.Int {
	// Calculate 2^(i-1)
	intervalSize := new(big.Int).Exp(big.NewInt(2), big.NewInt(int64(i-1)), nil)

	// Add decimal(id) + 2^(i-1)
	result := new(big.Int).Add(decimalID, intervalSize)

	// Mod 2^HASH_SIZE
	mod := new(big.Int).Exp(big.NewInt(2), big.NewInt(int64(HASH_SIZE)), nil)
	result.Mod(result, mod)

	return result
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
	case "GetPredecessor":
		fmt.Println(thisPeer.Port, "returns pred:", thisPeer.predecessor.Port)
		encoder.Encode(thisPeer.predecessor)
	case "SetPredecessor":
		var newPredecessor Peer
		json.Unmarshal(message.Data, &newPredecessor)
		thisPeer.predecessor = &newPredecessor
	case "GetFingertable":
		encoder.Encode(&thisPeer.fingerTable)
	case "GetClosestPrecedingFinger":
		var id big.Int
		json.Unmarshal(message.Data, &id)
		closest := thisPeer.closest_preceding_finger(id)
		encoder.Encode(closest)
	case "FindSuccessor":
		var id big.Int
		json.Unmarshal(message.Data, &id)
		successor := thisPeer.find_successor(id, thisPeer.successor)
		encoder.Encode(successor)
	case "UpdateFingertable":
		fmt.Println(thisPeer.Port + " is updating its fingertable")
		var rpc UpdateFingertableRpc
		json.Unmarshal(message.Data, &rpc)
		thisPeer.update_finger_table(&rpc.Peer, rpc.Index)
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
func (thisPeer *Peer) find_successor(id big.Int, successor *Peer) *Peer {
	oldPredecessor := thisPeer.find_predecessor(id)
	oldPredecessorAddr := oldPredecessor.IP + ":" + oldPredecessor.Port
	successorRes := &Peer{}
	if oldPredecessorAddr != thisPeer.IP+":"+thisPeer.Port {
		thisPeer.sendMessage(oldPredecessorAddr, "GetSuccessor", nil, successorRes)
	} else {
		return successor
	}
	return successorRes
}

// Find id's predecessor
func (thisPeer *Peer) find_predecessor(id big.Int) *Peer {
	currentClosest := thisPeer
	// call to get n'.successor
	currentClosestSuccessor := thisPeer.successor
	if currentClosestSuccessor == nil {
		succ := &Peer{}
		thisPeer.sendMessage(thisPeer.IP+":"+thisPeer.Port, "GetSuccessor", nil, succ)
		currentClosestSuccessor = succ
	}
	for !isBetweenUpperIncl(&id, &currentClosest.ID, &currentClosestSuccessor.ID) {
		currentClosestAddress := currentClosest.IP + ":" + currentClosest.Port
		newClosest := &Peer{}
		if currentClosestAddress == thisPeer.IP+":"+thisPeer.Port {
			if len(thisPeer.fingerTable) == 0 {
				return thisPeer
			}
			newClosest = thisPeer.closest_preceding_finger(id)
		} else {
			thisPeer.sendMessage(currentClosestAddress, "GetClosestPrecedingFinger", id, newClosest)
		}
		newCurrentClosestAddress := newClosest.IP + ":" + newClosest.Port
		newClosestSuccessor := &Peer{}
		if newCurrentClosestAddress == thisPeer.IP+":"+thisPeer.Port {
			newClosestSuccessor = thisPeer.successor
		} else {
			thisPeer.sendMessage(newCurrentClosestAddress, "GetSuccessor", nil, newClosestSuccessor)
		}

		// If the ID's didn't change, we have to return to not loop forever.
		if newClosest.ID.Cmp(&currentClosest.ID) == 0 &&
			currentClosestSuccessor.ID.Cmp(&newClosestSuccessor.ID) == 0 {
			// This if statement makes sure we return the correct of the two,
			// respecting the modulo in the circle.
			if id.Cmp(&currentClosestSuccessor.ID) < 0 {
				return currentClosest
			} else {
				return currentClosestSuccessor
			}
		}
		currentClosest = newClosest
		currentClosestSuccessor = newClosestSuccessor
	}
	return currentClosest
}

// Return closes preceding finger
func (thisPeer *Peer) closest_preceding_finger(id big.Int) *Peer {
	thisPeer.connMutex.Lock()
	defer thisPeer.connMutex.Unlock()
	for i := HASH_SIZE - 1; i >= 0; i-- {
		fingerNode := thisPeer.fingerTable[i].peer
		if isBetween(&fingerNode.ID, &thisPeer.ID, &id) {
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
		fmt.Println("Peer", thisPeer.ID.String(), "Successfully joined the network.")
	} else { // thisPeer is the only peer in the network
		thisPeer.connMutex.Lock()
		// think this is done earlier when the peer is created...
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
	// call for finger[1].node = n'.find_successor(finger[1].start)
	successor := &Peer{}
	thisPeer.sendMessage(existingPeer.IP+":"+existingPeer.Port, "FindSuccessor",
		&thisPeer.fingerTable[1].interval.iStart, successor)
	thisPeer.fingerTable[0].peer = *successor
	thisPeer.successor = successor
	successorAddr := successor.IP + ":" + successor.Port
	// call for thisPeer.predecessor = thisPeer.successor.predecessor
	SuccPred := &Peer{}
	thisPeer.sendMessage(successorAddr, "GetPredecessor", nil, SuccPred)
	thisPeer.predecessor = SuccPred
	// call for thisPeer.successor.predecessor = thisPeer
	thisPeer.sendMessage(successorAddr, "SetPredecessor", thisPeer, nil)
	for i := 0; i < HASH_SIZE-1; i++ {
		nextFinger := thisPeer.fingerTable[i+1].start
		if isBetweenLowerIncl(&nextFinger, &thisPeer.ID, &thisPeer.fingerTable[i].peer.ID) {
			thisPeer.fingerTable[i+1].peer = thisPeer.fingerTable[i].peer
		} else {
			findSuccResp := &Peer{}
			thisPeer.sendMessage(existingPeer.IP+":"+existingPeer.Port, "FindSuccessor", nextFinger, findSuccResp)
			if !(isBetweenLowerIncl(&findSuccResp.ID, &nextFinger, &thisPeer.ID)) {
				thisPeer.fingerTable[i+1].peer = *thisPeer
			} else {
				thisPeer.fingerTable[i+1].peer = *findSuccResp
			}
		}
	}
	thisPeer.connMutex.Unlock()
}

// update all nodes whose finger tables should refer to thisPeer
func (thisPeer *Peer) update_others() {
	fmt.Println("updating others finger tables...")
	otherAddress := ""
	for i := 0; i < HASH_SIZE; i++ {
		// compute id - 2^{i-1}
		offsetI := new(big.Int).Exp(big.NewInt(2), big.NewInt(int64(i)), nil)
		idMinusOffset := new(big.Int).Sub(&thisPeer.ID, offsetI)
		p := thisPeer.find_predecessor(*idMinusOffset)
		// call for p.update_finger_table(thisPeer, i)
		if thisPeer.IP+":"+thisPeer.Port == otherAddress {
			continue
		}
		otherAddress = p.IP + ":" + p.Port
		updateFingerTableRpc := UpdateFingertableRpc{
			Peer:  *thisPeer,
			Index: i,
		}
		thisPeer.sendMessage(otherAddress, "UpdateFingertable", &updateFingerTableRpc, nil)
		// time.Sleep(100 * time.Millisecond)
	}
}

// if s is ith finger of thisPeer, update thisPeers' finger table with s
func (thisPeer *Peer) update_finger_table(s *Peer, i int) {
	// If s \in [n, finger[i].node)
	// or if n = finger[i].node and s \in [finger[i].start, node)
	if isBetweenLowerIncl(&s.ID, &thisPeer.ID, &thisPeer.fingerTable[i].peer.ID) ||
		(thisPeer.ID.Cmp(&thisPeer.fingerTable[i].peer.ID) == 0 &&
			isBetweenLowerIncl(&s.ID, &thisPeer.fingerTable[i].start, &thisPeer.ID)) {
		fmt.Println("should update finger idx", i)
		thisPeer.fingerTable[i].peer = *s
		p := thisPeer.predecessor
		updateFingerTableRpc := &UpdateFingertableRpc{Peer: *s, Index: i}
		if p.IP+":"+p.Port != s.IP+":"+s.Port {
			thisPeer.sendMessage(p.IP+":"+p.Port, "UpdateFingertable", updateFingerTableRpc, nil)
		}
	} else {
		fmt.Println(&s.ID, &thisPeer.ID, &thisPeer.fingerTable[i].peer.ID)
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
	if response != nil {
		decoder.Decode(response)
	}
}

func printFingertable(fingers []Finger) {
	fmt.Println("Printing fingertable...")
	lastIdx := 0
	lastId := fingers[0].peer.ID
	for i, finger := range fingers {
		if finger.peer.ID.Cmp(&lastId) != 0 {
			fmt.Printf("%d-%d: port=%s id=%v\n", lastIdx, i-1, fingers[i-1].peer.Port, fingers[i-1].peer.ID.String())
			if i != HASH_SIZE-1 {
				lastId = fingers[i].peer.ID
				lastIdx = i
			}
		}
		if i == HASH_SIZE-1 {
			fmt.Printf("%d-%d: port=%s id=%v\n", lastIdx, i, fingers[i-1].peer.Port, fingers[i-1].peer.ID.String())
		}
	}
}

func isBetweenUpperIncl(num, lower, upper *big.Int) bool {
	if upper.Cmp(lower) >= 0 { // not affected by modulo
		return num.Cmp(lower) > 0 && num.Cmp(upper) <= 0
	} else { // affected by modulo
		return num.Cmp(lower) > 0 || num.Cmp(upper) <= 0
	}
}

func isBetweenLowerIncl(num, lower, upper *big.Int) bool {
	if upper.Cmp(lower) >= 0 { // not affected by modulo
		return num.Cmp(lower) >= 0 && num.Cmp(upper) < 0
	} else { // affected by modulo
		return num.Cmp(lower) >= 0 || num.Cmp(upper) < 0
	}
}

func isBetween(num, lower, upper *big.Int) bool {
	if upper.Cmp(lower) >= 0 { // not affected by modulo
		return num.Cmp(lower) > 0 && num.Cmp(upper) < 0
	} else { // affected by modulo
		return num.Cmp(lower) > 0 || num.Cmp(upper) < 0
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
		fmt.Println("Peer", peer.ID.String(), "is joining the network through", connectPeer.ID.String())
		peer.join(connectPeer)
	} else {
		peer.join(nil)
	}
	peer.Menu()
}
