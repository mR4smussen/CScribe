package main

import (
	"bufio"
	"crypto/rsa"
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
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
	groups      map[string]*Group
	Pk          *rsa.PublicKey
	sk          *rsa.PrivateKey
}

// Struct for a message
type Message struct {
	Type string
	Data []byte
}

// Initializes a new peer
func NewPeer(IP, Port string) *Peer {
	// Compute the unique ID for the IP address
	hash := sha1.New()
	hash.Write([]byte(IP + ":" + Port))
	hashBytes := hash.Sum(nil)
	ID := new(big.Int).SetBytes(hashBytes[:])
	sk := generateRSAKeys(4096)
	peer := Peer{
		ID:          *ID,
		IP:          IP,
		Port:        Port,
		fingerTable: []Finger{},
		successor:   &Peer{},
		predecessor: &Peer{},
		groups:      map[string]*Group{},
		Pk:          &sk.PublicKey,
		sk:          sk,
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
	// wrap around (mod circle size)
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
		// should this be a go routine?
		go thisPeer.handleConn(conn)
	}
}

func (thisPeer *Peer) handleConn(conn net.Conn) {
	encoder := json.NewEncoder(conn)
	decoder := json.NewDecoder(conn)
	var message Message
	err := decoder.Decode(&message)
	if err != nil {
		conn.Close()
		fmt.Printf("Got an error while decoding:%v\n", err)
	}
	thisPeer.handleMessage(encoder, &message)
	conn.Close()
}

func (thisPeer *Peer) handleMessage(encoder *json.Encoder, message *Message) {
	switch message.Type {
	case "GetPeer":
		encoder.Encode(thisPeer)
	case "GetSuccessor":
		encoder.Encode(thisPeer.successor)
	case "GetPredecessor":
		encoder.Encode(thisPeer.predecessor)
	case "SetPredecessor":
		var newPredecessor Peer
		json.Unmarshal(message.Data, &newPredecessor)
		thisPeer.predecessor = &newPredecessor
		// if the successor has not been updated,
		// then the new predecessor should also be the successor.
		if thisPeer.successor.ID.Cmp(&thisPeer.ID) == 0 {
			thisPeer.successor = &newPredecessor
		}
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
	case "Notify":
		var nPrime Peer
		json.Unmarshal(message.Data, &nPrime)
		thisPeer.notify(&nPrime)
	case "UpdateFingertable":
		var rpc UpdateFingertableRpc
		json.Unmarshal(message.Data, &rpc)
		thisPeer.update_finger_table(&rpc.Peer, rpc.Index)
	case "JoinGroup":
		var rpc joinRpc
		json.Unmarshal(message.Data, &rpc)
		root := thisPeer.forwardJoin(rpc)
		encoder.Encode(root)
	case "GetKey":
		var rpc requestKeyRpc
		json.Unmarshal(message.Data, &rpc)
		fmt.Println("got key request", rpc)
		gKey := thisPeer.groups[rpc.GroupId.String()].groupKey
		encryptedGroupKey := encryptRSA(gKey, rpc.Requestee.Pk)
		encoder.Encode(encryptedGroupKey)
	case "Multicast":
		var rpc multicastRpc
		json.Unmarshal(message.Data, &rpc)
		thisPeer.forwardMulticast(rpc)
	case "DrawRing":
		var senderPort string
		json.Unmarshal(message.Data, &senderPort)
		updateGraphLog(thisPeer.Port, thisPeer.successor.Port)
		if senderPort != thisPeer.Port {
			thisPeer.sendMessage(thisPeer.successor.IP+":"+thisPeer.successor.Port, "DrawRing", &senderPort, nil)
		}
	case "NotifyPredLeave": // used by thisPeer's pred to notify that they leave the network
		var leaveRpc LeaveRpc
		json.Unmarshal(message.Data, &leaveRpc)
		if leaveRpc.Leaver.ID.Cmp(&thisPeer.predecessor.ID) == 0 { // was send by our pred.
			// set our new pred to be the old predecessors pred
			fmt.Println(leaveRpc.Leaver.Port, "notified their succ:", thisPeer.Port, "that they are leaving")
			thisPeer.predecessor = &leaveRpc.NewConnection
		}
	case "NotifySuccLeave": // used by thisPeer's succ to notify that they leave the network
		var leaveRpc LeaveRpc
		json.Unmarshal(message.Data, &leaveRpc)
		if leaveRpc.Leaver.ID.Cmp(&thisPeer.successor.ID) == 0 { // was send by our succ.
			fmt.Println(leaveRpc.Leaver.Port, "notified their pred:", thisPeer.Port, "that they are leaving")
			// set our new succ to be the old successors succ
			thisPeer.successor = &leaveRpc.NewConnection
			// Update all fingers before the new succ to point to the new succ
			idx := 0
			for {
				if isBetween(&thisPeer.fingerTable[idx].start, &leaveRpc.NewConnection.ID, &thisPeer.ID) {
					break
				}
				thisPeer.fingerTable[idx].peer = leaveRpc.NewConnection
				idx = idx + 1
				if idx >= HASH_SIZE { // edge case: newConnection should fill up entire table
					break
				}
			}
		}
	}
}

// Menu to display options and take input
func (p *Peer) Menu() {
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Println("\n##################################")
		fmt.Println("####### Welcome to Schorbe #######")
		fmt.Println("##################################")
		fmt.Println("Choose an option: ")
		fmt.Println("1. Print fingertable")
		fmt.Println("2. Print fingertable loud")
		fmt.Println("3. Exit")
		fmt.Println("4. Create group <group name>")
		fmt.Println("5. Join group <root ip/group name>")
		fmt.Println("6. Send multicast <group name> <message>")
		fmt.Println("7. Draw ring")

		args, _ := reader.ReadString('\n')
		choice := strings.Split(args, " ")[0]
		choice = strings.TrimSpace(choice)

		switch choice {
		case "1":
			printFingertable(p.fingerTable)

		case "2":
			printFingertableLoud(p.fingerTable)

		case "3":
			fmt.Println("Exiting...")
			if p.successor != nil && p.successor.ID.Cmp(&p.ID) != 0 {
				// notify pred about leave
				leaveRpc := LeaveRpc{
					Leaver:        *p,
					NewConnection: *p.successor,
				}
				p.sendMessage(p.predecessor.IP+":"+p.predecessor.Port, "NotifySuccLeave", &leaveRpc, nil)
			}
			if p.predecessor != nil && p.predecessor.ID.Cmp(&p.ID) != 0 {
				// notify succ about leave
				leaveRpc := LeaveRpc{
					Leaver:        *p,
					NewConnection: *p.predecessor,
				}
				p.sendMessage(p.successor.IP+":"+p.successor.Port, "NotifyPredLeave", &leaveRpc, nil)
			}
			os.Exit(0)
		case "4":
			if len(strings.Split(args, " ")) < 2 {
				fmt.Println("make sure to include a name for the group.")
				fmt.Println("For instance \"4 foo\"")
				continue
			}
			name := strings.Split(args, " ")[1]
			p.create(strings.TrimSpace(name))
		case "5":
			if len(strings.Split(args, " ")) < 2 {
				fmt.Println("make sure to include the adrress of the root peer and name of the group.")
				fmt.Println("For instance \"5 localhost:xxxx/foo\".")
				continue
			}
			ids := strings.Split(args, " ")[1]
			if len(strings.Split(ids, "/")) < 2 {
				fmt.Println("the <addr>/<group name> should have the form:\n localhost:xxxx/foo")
				continue
			}
			p.joinGroup(strings.TrimSpace(ids))
		case "6":
			if len(strings.Split(args, " ")) < 3 {
				fmt.Println("make sure to include a group name and msg for the multicast.")
				fmt.Println("For instance: 6 golf hello golf members")
				continue
			}
			gname := strings.Split(args, " ")[1]
			msgSlice := strings.Split(args, " ")[2:]
			msg := strings.Join(msgSlice, " ")
			p.sendMulticast(gname, strings.TrimSpace(msg))
		case "7":
			logFile := filepath.Join("..", "logs", "network.md")
			os.WriteFile(logFile,
				[]byte(
					fmt.Sprintf("```mermaid\ngraph BT;\n")), 0644)
			if p.successor.ID.Cmp(&p.ID) != 0 {
				p.sendMessage(p.successor.IP+":"+p.successor.Port, "DrawRing", &p.Port, nil)
			}

		default:
			fmt.Println("Invalid option, please try again.")
		}
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

// Connect to another peer
// try untill we have a connection or untill max tries
func (thisPeer *Peer) GetConnection(otherAddr string) net.Conn {
	maxTries := 10
	tries := 0
	for {
		conn, err := net.Dial("tcp", otherAddr)
		if err != nil && tries >= maxTries {
			log.Printf("Error connecting to peer: %s %v, updating all fingers with this peer in it...", otherAddr, err)
			return nil
		} else if err == nil {
			if tries > 0 {
				fmt.Println("found connectiong!")
			}
			return conn
		}
		fmt.Println(thisPeer.Port, "wasn't able to connect to", otherAddr, "trying again...")
		tries++
		time.Sleep(500 * time.Millisecond)
	}
}

func startPeer(listenPort, connectPort string, isTest bool) {
	peer := NewPeer("localhost", listenPort)
	go peer.Listen()

	if connectPort != "0" {
		// Get n'
		connectAddr := "localhost:" + connectPort
		nPrime := &Peer{}
		peer.sendMessage(connectAddr, "GetPeer", nil, nPrime)

		// connect to the network through n'
		if !isTest {
			fmt.Println("Peer", peer.ID.String(), "is joining the network through", nPrime.ID.String())
		}
		peer.join(nPrime)
	} else {
		peer.join(nil)
	}

	if !isTest {
		peer.Menu()
	} else {
		select {}
	}
}

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: go run . [listen_port] [connect_port]")
		fmt.Println("To create a new network, use \"0\" as the [connect_port].")
		os.Exit(1)
	}
	if os.Args[1] == "test" {
		if len(os.Args) == 3 {
			fmt.Println("To run the tests, use: go run . test <amount> <firstPort>")
			os.Exit(1)
		}
		test(os.Args[2], os.Args[3])
		return
	}

	listenPort := os.Args[1]
	connectPort := os.Args[2]
	startPeer(listenPort, connectPort, false)
}
