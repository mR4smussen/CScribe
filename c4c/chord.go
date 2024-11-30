package main

/*
This file implements the P2P chord functionality described in the paper:
https://dl.acm.org/doi/10.1145/964723.383071
*/

import (
	"fmt"
	"math/big"
	"math/rand"
	"os"
	"time"
)

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

type UpdateFingertableRpc struct {
	Peer  Peer
	Index int
}

type LeaveRpc struct {
	Leaver        Peer
	NewConnection Peer
}

// Ask node n to find id's successor
func (n *Peer) find_successor(id big.Int, nSuccessor *Peer) *Peer {
	nPrime := n.find_predecessor(id)

	// Call to get n'.succeesor
	successorRes := &Peer{}
	if nPrime.ID.Cmp(&n.ID) == 0 {
		return nSuccessor
	} else {
		n.sendMessage(nPrime.IP+":"+nPrime.Port, "GetSuccessor", nil, successorRes)
	}
	return successorRes
}

// Ask node n to find id's predecessor
func (n *Peer) find_predecessor(id big.Int) *Peer {
	// edge case 1: n only has itself in the fingertable
	// -> return n
	// edge case 2: if n1 has closest_preceding_finger to be n2 and n2 has closest_preceding_finger to be n1
	// -> return the first of n1 and n2 before `id`

	// define n' = n
	nPrime := n

	// Get n'.successor
	nPrimeSucc := n.successor
	if nPrimeSucc == nil {
		succ := &Peer{}
		n.sendMessage(n.IP+":"+n.Port, "GetSuccessor", nil, succ)
		nPrimeSucc = succ
	}

	// while id \not\in (n', n'.successor]
	for !isBetweenUpperIncl(&id, &nPrime.ID, &nPrimeSucc.ID) {
		// get n'.closest_preceding_finger(id)
		closestPrecFinger := &Peer{}
		if nPrime.ID.Cmp(&n.ID) == 0 {
			closestPrecFinger = n.closest_preceding_finger(id)
		} else {
			n.sendMessage(nPrime.IP+":"+nPrime.Port, "GetClosestPrecedingFinger", &id, closestPrecFinger)
		}

		// Edge case 1
		if closestPrecFinger.ID.Cmp(&n.ID) == 0 {
			return n
		}

		// Get new n'.successor
		closestPrecFingerSucc := &Peer{}
		n.sendMessage(closestPrecFinger.IP+":"+closestPrecFinger.Port, "GetSuccessor", nil, closestPrecFingerSucc)

		// Edge case 2
		if nPrime.ID.Cmp(&closestPrecFingerSucc.ID) == 0 && nPrime.ID.Cmp(&closestPrecFinger.ID) == 0 {
			if isBetween(&id, &nPrime.ID, &nPrimeSucc.ID) {
				return nPrime
			} else {
				return nPrimeSucc
			}
		}

		// update local n' and n'.successor
		nPrime = closestPrecFinger
		nPrimeSucc = closestPrecFingerSucc
	}
	// return n'
	return nPrime
}

// Return closes finger preceding id
func (n *Peer) closest_preceding_finger(id big.Int) *Peer {
	n.connMutex.Lock()
	defer n.connMutex.Unlock()
	for i := HASH_SIZE - 1; i >= 0; i-- {
		fingerNode := n.fingerTable[i].peer
		if isBetween(&fingerNode.ID, &n.ID, &id) {
			return &fingerNode
		}
	}
	return n
}

// node n joins the network;
// n' is an arbitrary node in the network
func (n *Peer) join(nPrime *Peer) {
	n.predecessor = nil
	n.successor = n
	if nPrime != nil {
		// call for n'.find_successor(n)
		successor := &Peer{}
		n.sendMessage(nPrime.IP+":"+nPrime.Port, "FindSuccessor", &n.ID, successor)
		n.successor = successor
		n.fingerTable[0].peer = *n.successor

		fmt.Println("Peer ("+n.Port+")", n.ID.String(), "Successfully joined the network.")
		os.WriteFile("peer_lock.txt", []byte("open"), 0644)
	}

	// start periodic stabilize() and fix_fingers().
	quitStabilize := make(chan struct{})
	quitFixFingers := make(chan struct{})
	go startPeriodicTask(n.stabilize, 1*time.Second, quitStabilize)
	go startPeriodicTask(n.fix_fingers, 3*time.Second, quitFixFingers)
}

// periodically verify n's immediate successor,
// and tell the successor about n.
func (n *Peer) stabilize() {
	// edge case: If n = n.successor then we don't need the remote call.
	if n.ID.Cmp(&n.successor.ID) == 0 {
		if n.predecessor != nil {
			n.successor = n.predecessor
		}
		return
	}
	// call to get successor.predecessor
	// note: the paper will have the successor.predecessor stored locally... we don't
	x := &Peer{}
	succIP := n.successor.IP + ":" + n.successor.Port
	n.sendMessage(succIP, "GetPredecessor", nil, x)
	if isBetween(&x.ID, &n.ID, &n.successor.ID) && x.Port != "" {
		n.successor = x
	}
	// call for successor.notify(n)
	n.sendMessage(n.successor.IP+":"+n.successor.Port, "Notify", &n, nil)
}

// n' thinks it might be n's predecessor.
func (n *Peer) notify(nPrime *Peer) {
	if n.predecessor == nil || isBetween(&nPrime.ID, &n.predecessor.ID, &n.ID) {
		fmt.Println(nPrime.Port, "told", n.Port, "that they are their predecessor.")
		n.predecessor = nPrime
	}
}

// periodically refresh finger table entries.
// Note this is not really the optimal way, hence we have change it a bit.
// (the paper only uses this to argue correctness, not efficiency).
func (n *Peer) fix_fingers() {
	i := rand.Intn(HASH_SIZE)
	n.fix_finger(i)
}

func (n *Peer) fix_finger(i int) {
	succOfFingerI := *n.find_successor(n.fingerTable[i].start, n.successor)
	n.fingerTable[i].peer = succOfFingerI
	pickedIdx := i
	// all fingers between finger[i].start and succOfFingerI should point to succOfFingerI.
	for {
		// next finger idx
		i = (i + 1) % HASH_SIZE
		if isBetween(&n.fingerTable[i].start, &succOfFingerI.ID, &n.fingerTable[pickedIdx].start) {
			break
		}
		n.fingerTable[i].peer = succOfFingerI
	}
}

// General function to start a periodic task
func startPeriodicTask(task func(), interval time.Duration, quit chan struct{}) {
	ticker := time.NewTicker(interval)
	defer close(quit)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			task()
		case <-quit:
			return
		}
	}
}

// if s is ith finger of n, update n's finger table with s
func (n *Peer) update_finger_table(s *Peer, i int) {
	// If s \in [n, finger[i].node)
	// or if n = finger[i].node and s \in [finger[i].start, n)
	if isBetweenLowerIncl(&s.ID, &n.ID, &n.fingerTable[i].peer.ID) ||
		(n.ID.Cmp(&n.fingerTable[i].peer.ID) == 0 &&
			isBetweenLowerIncl(&s.ID, &n.fingerTable[i].start, &n.ID)) {
		n.fingerTable[i].peer = *s

		// Call to get first node predecing n
		p := n.predecessor
		updateFingerTableRpc := &UpdateFingertableRpc{Peer: *s, Index: i}

		// call for p.update_finger_table(s, i) if p != s
		if p.IP+":"+p.Port != s.IP+":"+s.Port {
			n.sendMessage(p.IP+":"+p.Port, "UpdateFingertable", updateFingerTableRpc, nil)
		}
	}
	n.successor = &n.fingerTable[0].peer
}
