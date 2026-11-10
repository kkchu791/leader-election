package main

import (
	"fmt"
	"sync"
	"time"
)

type Vote struct {
	kind    string
	node    string
	term    int
	success bool
}

type Node struct {
	name    string
	kind    string
	term    int
	votes   int
	voteFor string
	ch      chan Vote
	peers   map[string]Node
}

func process(n Node, ticker *time.Ticker) {
	for {
		select {
		case <-ticker.C:
			fmt.Println("election timeout ", n.name)
			// becoming a candidate
			n.kind = "candidate"

			// updates the term
			n.term++

			// votes for itself
			n.votes++

			//sending to all other nodes VoteReq
			for _, peer := range n.peers {
				peer.ch <- Vote{
					kind: "request",
					node: n.name,
					term: n.term,
				}
			}

		case vote := <-n.ch:
			if vote.kind == "request" {
				//this is getting a requestVote and sending out a responseVote
				if vote.term > n.term {
					n.voteFor = ""
				}

				if n.voteFor == "" && vote.term >= n.term {
					// adopt the new term
					n.term = vote.term

					// update the node voteFor so it can't vote again during this term
					n.voteFor = vote.node

					// send voteResp back to candidate
					voteNode := n.peers[vote.node]
					voteNode.ch <- Vote{
						kind:    "response",
						node:    n.name,
						term:    n.term,
						success: true,
					}
				}

				if vote.term < n.term {

					//downgrade this node in case its current a follower or leader
					n.kind = "follower"

					voteNode := n.peers[vote.node]
					voteNode.ch <- Vote{
						kind:    "response",
						node:    n.name,
						term:    n.term,
						success: false,
					}
				}
			}

			if vote.kind == "response" {
				// majority check : votes > len(peers)/2
				if vote.success == true {
					n.votes++
				}

				if n.votes > len(n.peers)/2 {
					if n.kind == "candidate" {
						fmt.Println(n.name, " becomes a leader")
						n.kind = "leader"
					}
				}
			}
		}
	}

}

func main() {
	var wg sync.WaitGroup

	nodeA := Node{
		name:    "A",
		kind:    "follower",
		term:    0,
		voteFor: "",
		ch:      make(chan Vote, 10),
		peers:   make(map[string]Node, 0),
	}

	nodeB := Node{
		name:    "B",
		kind:    "follower",
		term:    0,
		voteFor: "",
		ch:      make(chan Vote, 10),
		peers:   make(map[string]Node, 0),
	}

	nodeC := Node{
		name:    "C",
		kind:    "follower",
		term:    0,
		voteFor: "",
		ch:      make(chan Vote, 10),
		peers:   make(map[string]Node, 0),
	}

	nodeA.peers[nodeB.name] = nodeB
	nodeA.peers[nodeC.name] = nodeC
	nodeB.peers[nodeA.name] = nodeA
	nodeB.peers[nodeC.name] = nodeC
	nodeC.peers[nodeA.name] = nodeA
	nodeC.peers[nodeB.name] = nodeB

	wg.Go(func() {
		tickerA := time.NewTicker(1600 * time.Millisecond)
		process(nodeA, tickerA)
	})

	wg.Go(func() {
		tickerB := time.NewTicker(3000 * time.Millisecond)
		process(nodeB, tickerB)
	})

	wg.Go(func() {
		tickerC := time.NewTicker(5100 * time.Millisecond)
		process(nodeC, tickerC)
	})

	wg.Wait()
}
