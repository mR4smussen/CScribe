package main

import (
	"fmt"
	"math/big"
)

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
