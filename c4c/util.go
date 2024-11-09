package main

import (
	"crypto/sha1"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"time"
)

func hashString(s string) *big.Int {
	hash := sha1.New()
	hash.Write([]byte(s))
	hashBytes := hash.Sum(nil)
	return new(big.Int).SetBytes(hashBytes[:])
}

// Log a message to the group "gname"
func glog(gname, msg string) {
	logFile := filepath.Join("..", "logs", gname+"_log.md")
	file, err := os.OpenFile(logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("opening log file", gname+"_log failed:\n", err)
		return
	}
	defer file.Close()

	// we keep trying to write to the file until we succeed.
	for {
		_, err = file.Write([]byte(msg + "\n"))
		if err == nil {
			break
		}
		time.Sleep(time.Second)
	}
}

func isBetween(num, lower, upper *big.Int) bool {
	if upper.Cmp(lower) >= 0 { // not affected by modulo
		return num.Cmp(lower) > 0 && num.Cmp(upper) < 0
	} else { // affected by modulo
		return num.Cmp(lower) > 0 || num.Cmp(upper) < 0
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

func (n *Peer) bestFingerForLookup(id *big.Int) *Peer {
	for i := HASH_SIZE - 1; i >= 0; i-- {
		currentBest := n.fingerTable[i]
		if isBetweenUpperIncl(&n.fingerTable[i].peer.ID, &n.ID, id) {
			return &currentBest.peer
		}
	}
	return n
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
			fmt.Printf("-%d: port=%s id=%v\n", i, finger.peer.Port, finger.peer.ID.String())
		}
	}
}

func printFingertableLoud(fingers []Finger) {
	fmt.Println("Printing fingertable...")
	for i, _ := range fingers {
		fmt.Printf("%d: (start: %v): port = %s id = %v\n", i, fingers[i].start.String(), fingers[i].peer.Port, fingers[i].peer.ID.String())
	}
}
