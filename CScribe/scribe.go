package main

import (
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"strings"
)

type Group struct {
	children []*Peer
	root     *Peer
	groupKey []byte // the aes key and nonce used for this group
	gName    string // only used for logging
}

type joinRpc struct {
	PeerId    big.Int // The id of the creater of the group
	GroupId   big.Int // The id of the group name
	Forwarder *Peer   // The peer forwarding the message (to add to the tree)
	Sender    *Peer   // The original sender of the message
	GroupName string  // Just used for logging to the correct file
}

type multicastRpc struct {
	GroupName        string
	EncryptedMessage []byte
	Sender           *Peer
}

type requestKeyRpc struct {
	GroupId   big.Int
	Requestee *Peer
}

// Creates a group with an ID made from the name and n's ID.
func (n *Peer) create(name string) {
	ID := hashString(name)
	if n.groups[ID.String()] == nil {
		groupKey := generateAESKey()
		n.groups[ID.String()] = &Group{
			children: []*Peer{},
			root:     n,
			groupKey: groupKey,
			gName:    name,
		}
		logFile := filepath.Join("..", "logs", name+"_log.md")

		os.WriteFile(logFile,
			[]byte(
				fmt.Sprintf("### Peer (%s) created the group: %s\n```mermaid\ngraph BT;\n", n.Port, name)), 0644)
		fmt.Println("\nGroup \"" + name + "\" created")
	} else {
		fmt.Println("Group \""+name+"\" already exists with id ", ID.String())
	}
}

// Use chord to send a join message to the peer in the ID
// This method is called from the menu
// Note: currently each peer can not be in two groups with the same name.
func (n *Peer) joinGroup(domain string) {
	ids := strings.Split(domain, "/")
	peerId := hashString(ids[0])
	groupId := hashString(ids[1])
	if peerId.Cmp(&n.ID) == 0 {
		fmt.Println("You can't join your own group.")
		return
	}
	group := n.groups[groupId.String()]
	if group != nil {
		if group.groupKey == nil {
			// Use the root to get the symmetric group key
			requestRpc := requestKeyRpc{
				GroupId:   *hashString(ids[1]),
				Requestee: n,
			}
			encryptedGroupKey := &[]byte{}
			n.sendMessage(group.root.IP+":"+group.root.Port, "GetKey", &requestRpc, encryptedGroupKey)
			gKey := decryptRSA(*encryptedGroupKey, n.sk)
			n.groups[groupId.String()] = &Group{
				children: group.children,
				root:     group.root,
				groupKey: gKey,
				gName:    ids[1],
			}
		} else {
			fmt.Println("You are already part of this group.")
		}
		return
	}

	bestFinger := n.bestFingerForLookup(peerId)
	if bestFinger.ID.Cmp(&n.ID) == 0 {
		// If n is the best peer for this lookup and n doesn't know about the group, then something is wrong
		fmt.Println("Chord was not able to find the root of the group.")
		fmt.Println("Wait for the network to stabilize before trying again.")
	} else {
		// join the group tree (get the root)
		joinRpc := joinRpc{
			PeerId:    *peerId,
			GroupId:   *groupId,
			Forwarder: n,
			Sender:    n,
			GroupName: ids[1],
		}
		root := &Peer{}
		n.sendMessage(bestFinger.IP+":"+bestFinger.Port, "JoinGroup", &joinRpc, root)

		// Use the root to get the symmetric group key
		requestRpc := requestKeyRpc{
			GroupId:   *hashString(ids[1]),
			Requestee: n,
		}
		encryptedGroupKey := &[]byte{}
		n.sendMessage(root.IP+":"+root.Port, "GetKey", &requestRpc, encryptedGroupKey)
		gKey := decryptRSA(*encryptedGroupKey, n.sk)
		n.groups[groupId.String()] = &Group{
			children: []*Peer{},
			root:     root,
			groupKey: gKey,
			gName:    ids[1],
		}
		glog(ids[1], fmt.Sprintf("	%s((%s))-->%s((%s));",
			n.Port, n.Port, bestFinger.Port, bestFinger.Port))
		fmt.Println("\nsuccessfully joined group", n.groups[groupId.String()].gName)
	}
}

// Forward a join message
// This method is called when receiving a join message
func (n *Peer) forwardJoin(rpc joinRpc) *Peer {
	groupId := rpc.GroupId.String()
	if n.groups[groupId] == nil { // Route the message foward
		bestFinger := n.bestFingerForLookup(&rpc.PeerId)
		if bestFinger.ID.Cmp(&n.ID) == 0 {
			fmt.Println("Chord was not able to find the root of the group.")
			fmt.Println("Wait for the network to stabilize before trying again.")
			return nil
		} else { // Forward the join message to the best finger
			nextRpc := joinRpc{
				PeerId:    rpc.PeerId,
				GroupId:   rpc.GroupId,
				Forwarder: n,
				Sender:    rpc.Sender,
				GroupName: rpc.GroupName,
			}
			root := &Peer{}
			n.sendMessage(bestFinger.IP+":"+bestFinger.Port,
				"JoinGroup", &nextRpc, root)

			// Join the group tree
			n.groups[groupId] = &Group{
				children: []*Peer{rpc.Forwarder},
				root:     root,
				groupKey: nil,
				gName:    rpc.GroupName,
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
		return n.groups[groupId].root
	}
}

// Send a message to the root of the group, who will then parse it down the tree.
func (n *Peer) sendMulticast(gname, msg string) {
	groupId := hashString(gname)
	group := n.groups[groupId.String()]
	if group == nil {
		fmt.Println("You are not a member of", gname)
		return
	}
	root := group.root
	encryptedMessage := encryptAES([]byte(msg), group.groupKey)
	rpc := multicastRpc{
		GroupName:        gname,
		EncryptedMessage: encryptedMessage,
		Sender:           n,
	}
	if root.ID.Cmp(&n.ID) == 0 {
		n.forwardMulticast(rpc)
	} else {
		n.sendMessage(root.IP+":"+root.Port, "Multicast", &rpc, nil)
	}
}

// Forwards a multicast message parsed down from the parent
func (n *Peer) forwardMulticast(rpc multicastRpc) {
	groupId := hashString(rpc.GroupName)
	group := n.groups[groupId.String()]
	children := group.children

	// If n is in the group (has the group key) - log the message
	if group.groupKey != nil {
		decryptedMessage := decryptAES(rpc.EncryptedMessage, group.groupKey)
		logMsg := fmt.Sprintf("\n(%s:%s): \"%s\"", rpc.Sender.Port, rpc.GroupName, decryptedMessage)
		fmt.Println(logMsg)
	}
	for _, child := range children {
		n.sendMessage(child.IP+":"+child.Port, "Multicast", &rpc, nil)
	}
}
