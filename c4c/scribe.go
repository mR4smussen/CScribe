package main

import (
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"time"
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
	GroupName string  // Just used for logging to the correct file
}

// Creates a group with an ID made from the name and n's ID.
func (n *Peer) create(name string) {
	ID := hashString(name)
	if n.groups[ID.String()] == nil {
		n.groups[ID.String()] = &group{
			children: []*Peer{},
			root:     n,
		}
		logFile := filepath.Join("..", "logs", name+"_log.md")

		os.WriteFile(logFile,
			[]byte(
				fmt.Sprintf("### Peer (%s) created the group: %s\n```mermaid\ngraph BT;\n", n.Port, name)), 0644)
		fmt.Println("Group \""+name+"\" created with id ", ID.String())
	} else {
		fmt.Println("Group \""+name+"\" already exists with id ", ID.String())
	}
}

// Use chord to send a join message to the peer in the ID
// This method is called from the menu
func (n *Peer) joinGroup(ID string) {
	ids := strings.Split(ID, "/")
	peerId := hashString(ids[0])
	groupId := hashString(ids[1])
	if peerId.Cmp(&n.ID) == 0 {
		fmt.Println("You can't join your own group.")
		return
	}
	if n.groups[groupId.String()] != nil {
		fmt.Println("Seems you are already part of this group")
		// TODO - when we add creds, we need to add these to the group here...
		return
	}

	fmt.Println("Trying to connect to group", groupId.String(),
		"created by peer", peerId.String())

	bestFinger := n.bestFingerForLookup(peerId)
	if bestFinger.ID.Cmp(&n.ID) == 0 {
		// If n is the best peer for this lookup, then we check if n has the group.
		// Note, this is not the same as joining your own group, since it might be that
		// you are the root of a group without being part of the group?
		if n.groups[groupId.String()] == nil {
			fmt.Println("Seems you are the root, and have lost the group :(")
		} else {
			fmt.Println("Seems you are already a member of this group")
		}
	} else { // Else we send a join message to the "best" finger.
		joinRpc := joinRpc{
			PeerId:    *peerId,
			GroupId:   *groupId,
			Forwarder: n,
			Sender:    n,
			GroupName: ids[1],
		}
		fmt.Println("Trying to join group through", bestFinger.Port)
		root := &Peer{}
		n.sendMessage(bestFinger.IP+":"+bestFinger.Port,
			"JoinGroup", &joinRpc, root)
		n.groups[groupId.String()] = &group{
			children: []*Peer{},
			root:     root,
		}
		glog(ids[1], fmt.Sprintf("	%s((%s))-->%s((%s));",
			n.Port, n.Port, bestFinger.Port, bestFinger.Port))
		fmt.Println("successfully joined new group with root on port", n.groups[groupId.String()].root.Port)
	}
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
				GroupName: rpc.GroupName,
			}
			fmt.Println("Sending join", nextRpc, "to best peer:", bestFinger.Port)
			root := &Peer{}
			n.sendMessage(bestFinger.IP+":"+bestFinger.Port,
				"JoinGroup", &nextRpc, root)

			// Save the group
			n.groups[groupId] = &group{
				children: []*Peer{rpc.Forwarder},
				root:     root,
			}
			glog(rpc.GroupName, fmt.Sprintf("	%s((%s))-->%s((%s));",
				n.Port, n.Port, bestFinger.Port, bestFinger.Port))
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
