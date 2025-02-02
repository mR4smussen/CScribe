package main

import (
	"bufio"
	"crypto/sha1"
	"fmt"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"strconv"
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

func findSDOfChildren() {
	total := 0
	amount := 0
	networkChildrenFile := filepath.Join("..", "logs", "network_children.md")

	// Open the file
	file, err := os.Open(networkChildrenFile)
	if err != nil {
		fmt.Println("failed to open file: %w", err)
		return
	}
	defer file.Close()

	// Read all lines into a slice
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		children := strings.Split(line, ":")[1]
		childrenInt, _ := strconv.Atoi(children)
		total += childrenInt
		amount++
	}
	if err := scanner.Err(); err != nil {
		fmt.Errorf("failed to read file: %w", err)
		return
	}

	// compute SD
	mean := float64(total) / float64(amount)
	sum := float64(0)

	file2, err2 := os.Open(networkChildrenFile)
	if err2 != nil {
		fmt.Println("failed to open file: %w", err2)
		return
	}
	defer file2.Close()

	scanner2 := bufio.NewScanner(file2)
	for scanner2.Scan() {
		line := scanner2.Text()
		children := strings.Split(line, ":")[1]
		childrenInt, _ := strconv.Atoi(children)
		sum += math.Pow(float64(childrenInt)-float64(mean), float64(2))
	}
	if err := scanner2.Err(); err != nil {
		fmt.Errorf("failed to read file: %w", err)
		return
	}
	SD := math.Sqrt(sum / float64(amount))

	file, err = os.OpenFile(networkChildrenFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("was not able to open file", networkChildrenFile)
		return
	}
	defer file.Close()

	sdString := fmt.Sprintf("Mean:%f\nSD:%f\n", mean, SD)

	if _, err := file.WriteString(sdString); err != nil {
		fmt.Println("was not able to write to file", networkChildrenFile)
		return
	}

}

// Boot multiple peers at the same time
func bootPeers(amountString, firstPort string) {
	amount, err := strconv.Atoi(amountString)
	if err != nil {
		panic("when running tests the second argument should be a number")
	}
	fmt.Println("Booting", amount, "peers")

	basePort := 8000
	startPort, _ := strconv.Atoi(firstPort)

	for i := 1; i <= amount-1; i++ {
		time.Sleep(500 * time.Millisecond)
		listenPort := fmt.Sprintf("%d", startPort+i-1)
		connectPort := fmt.Sprintf("%d", basePort)
		go startPeer(listenPort, connectPort, true)
	}

	startPeer(fmt.Sprintf("%d", startPort+amount-1), fmt.Sprintf("%d", basePort), false)

	select {}
}
