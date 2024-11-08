package main

import (
	"crypto/sha1"
	"fmt"
	"math/big"
	"strings"
)

type group struct {
	children []*Peer
	root     *Peer
}

type joinRpc struct {
	PeerId    big.Int // The id of the creater of the group
	GroupId   big.Int // The ide of the group name
	Forwarder *Peer   // The peer forwarding the message (to add to the branch)
	Sender    *Peer   // The original sender of the message
}

// Creates a group with an ID made from the name and n's ID.
// TODO - make the ID be n's id extended with the hash of the name, not just the name
func (n *Peer) create(name string) {
	// Compute the ID for the name
	hash := sha1.New()
	hash.Write([]byte(name))
	hashBytes := hash.Sum(nil)
	ID := new(big.Int).SetBytes(hashBytes[:])
	if n.groups[ID.String()] == nil {
		n.groups[ID.String()] = &group{
			children: []*Peer{},
			root:     n,
		}
		fmt.Println("Group \""+name+"\" created with id ", ID.String())
	} else {
		fmt.Println("Group \""+name+"\" already exists with id ", ID.String())
	}
}

// Use chord to send a join message to the peer in the ID
// This method is called from the menu
func (n *Peer) joinGroup(ID string) {
	ids := strings.Split(ID, "/")
	peerId, _ := new(big.Int).SetString(ids[0], 10)
	groupId, _ := new(big.Int).SetString(ids[1], 10)
	if peerId.Cmp(&n.ID) == 0 {
		fmt.Println("You can't join to your own group.")
		return
	}

	fmt.Println("Trying to connect to group", groupId.String(),
		"created by peer", peerId.String())

	bestFinger := n.bestFingerForLookup(peerId)
	fmt.Println("best finger:", bestFinger.ID.String())
	if bestFinger.ID.Cmp(&n.ID) == 0 {
		// If n is the best peer for this lookup, then we check if n has the group.
		// Note, this is not the same as joining your own group, since it might be that
		// you are the root of a group without being part of the group?
		if n.groups[groupId.String()] == nil {
			fmt.Println("Seems you are the root, and have lost the group :(")
		} else {
			// do nothing? we already have the group... maybe only group members can be root?
		}
	} else { // Else we send a join message to the "best" finger.
		joinRpc := joinRpc{
			PeerId:    *peerId,
			GroupId:   *groupId,
			Forwarder: n,
			Sender:    n,
		}
		fmt.Println("Sending join", joinRpc, "to best peer:", bestFinger.Port)
		root := &Peer{}
		n.sendMessage(bestFinger.IP+":"+bestFinger.Port,
			"JoinGroup", &joinRpc, root)
		n.groups[groupId.String()] = &group{
			children: []*Peer{},
			root:     root,
		}
		fmt.Println("successfully joined new group with root on port", n.groups[groupId.String()].root.Port)
	}

	// TODO - make lookup on peerId:
	// 1. find peer in fingertable furthest towards peerId or the successor of the peerId
	// 2. send join message
	// 3. ... check groups?
}

// Forward a join message
// This method is called when receiving a join message
func (n *Peer) forwardJoin(rpc joinRpc) *Peer {
	groupId := rpc.GroupId.String()
	// If we don't forward all message, then we need each party in the tree to keep track of who the current root of the tree is
	// this is probably best, since this is what the pseudo code does...
	if n.groups[groupId] == nil { // Route the message foward
		bestFinger := n.bestFingerForLookup(&rpc.PeerId)
		if bestFinger.ID.Cmp(&n.ID) == 0 {
			fmt.Println("The root doesn't seem to know about the group...")
			return nil
		} else { // Forward join to best finger
			nextRpc := joinRpc{
				PeerId:    rpc.PeerId,
				GroupId:   rpc.GroupId,
				Forwarder: n,
				Sender:    rpc.Sender,
			}
			fmt.Println("Sending join", nextRpc, "to best peer:", bestFinger.Port)
			root := &Peer{}
			n.sendMessage(bestFinger.IP+":"+bestFinger.Port,
				"JoinGroup", &nextRpc, root)

			n.groups[groupId] = &group{
				children: []*Peer{rpc.Sender},
				root:     root,
			}
			return root
		}
	} else {
		// n is already part of the group tree
		// so we just add the forwarder to the children list and return the known root
		existingGroup := n.groups[groupId]
		newChildrenList := append(existingGroup.children, rpc.Forwarder)
		existingGroup.children = newChildrenList
		n.groups[groupId] = existingGroup
		fmt.Println("Already part of the tree, returning the root...", n.groups[groupId].root.Port)
		return n.groups[groupId].root
	}
}
