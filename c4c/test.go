package main

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// TODO - add logs, so that we can see who is connected to who.
// maybe use some kind of broadcast to ask all peers to log their
// "current" finger table once done connecting all peers?

// Test that connects a specific amount of peers, one at a time
func test(amountString, firstPort string) {
	amount, err := strconv.Atoi(amountString)
	if err != nil {
		panic("when running tests the second argument should be a number")
	}
	fmt.Println("Runnign test with", amount, "peers")

	basePort := 8000
	startPort, _ := strconv.Atoi(firstPort)

	lockFile := "peer_lock.txt"

	err = os.WriteFile(lockFile, []byte("open"), 0644)
	if err != nil {
		fmt.Println("Error creating lock file:", err)
		return
	}

	for i := 1; i <= amount-1; i++ {
		time.Sleep(500 * time.Millisecond)
		for {
			lock, err := os.ReadFile(lockFile)
			if err != nil {
				fmt.Println("Error reading lock file:", err)
				return
			}

			if string(lock) == "open" {
				os.WriteFile(lockFile, []byte("closed"), 0644)
				listenPort := fmt.Sprintf("%d", startPort+i-1)
				// TODO - use random port in [baseport, startPort+i-1)
				connectPort := fmt.Sprintf("%d", basePort)
				fmt.Println("Connecting peer with port", listenPort, "to peer with port", connectPort)
				go startPeer(listenPort, connectPort, true)
				break
			} else {
				time.Sleep(100 * time.Millisecond)
			}
		}
	}

	startPeer(fmt.Sprintf("%d", startPort+amount-1), fmt.Sprintf("%d", basePort), false)

	select {}
}
