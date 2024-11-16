package main

import (
	"bufio"
	"crypto/sha1"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"strings"
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

// change a connection in the log file
// remove the arrow going away from a, and adds an arrow from a to b
func updateGraphLog(a, b string) error {
	graphLogFile := filepath.Join("..", "logs", "network.md")

	// Open the file
	file, err := os.Open(graphLogFile)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	// Read all lines into a slice
	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		// Skip the line starting with the node `a`
		if strings.HasPrefix(line, fmt.Sprintf("\t%s(", a)) {
			continue
		}
		lines = append(lines, line)
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}

	// Add the new line from `a` to `b`
	lines = append(lines, fmt.Sprintf("\t%s((%s))-->%s((%s));", a, a, b, b))

	// Write the updated lines back to the file
	file, err = os.Create(graphLogFile)
	if err != nil {
		return fmt.Errorf("failed to reopen file for writing: %w", err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	for _, line := range lines {
		_, err := writer.WriteString(line + "\n")
		if err != nil {
			return fmt.Errorf("failed to write to file: %w", err)
		}
	}
	if err := writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush writer: %w", err)
	}

	return nil
}
