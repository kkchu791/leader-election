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

type Append struct {
	kind    string
	node    string
	term    int
	success bool
}

type Node struct {
	name     string
	kind     string
	term     int
	votes    int
	voteFor  string
	voteCh   chan Vote
	appendCh chan Append
	peers    map[string]Node
}

func process(n Node, electionTicker *time.Ticker) {
	appendTicker := time.NewTicker(1000 * time.Millisecond)
	appendTicker.Stop()

	for {
		select {
		case <-electionTicker.C:
			fmt.Println("election timeout ", n.name)
			// becoming a candidate
			n.kind = "candidate"

			//resets its vote count for new election
			n.votes = 0

			// updates the term
			n.term++

			// votes for itself
			n.voteFor = n.name
			n.votes++

			//sending to all other nodes VoteReq
			for _, peer := range n.peers {
				peer.voteCh <- Vote{
					kind: "request",
					node: n.name,
					term: n.term,
				}
			}

		case vote := <-n.voteCh:
			if vote.kind == "request" {

				//resetting my state to give a vote
				if vote.term > n.term {
					n.voteFor = ""
					n.kind = "follower"
				}

				if n.voteFor == "" && vote.term >= n.term {
					// adopt the new term
					n.term = vote.term

					// update the node voteFor so it can't vote again during this term
					n.voteFor = vote.node

					// send voteResp back to candidate
					voteNode := n.peers[vote.node]
					voteNode.voteCh <- Vote{
						kind:    "response",
						node:    n.name,
						term:    n.term,
						success: true,
					}
				}

				if vote.term < n.term {
					voteNode := n.peers[vote.node]
					voteNode.voteCh <- Vote{
						kind:    "response",
						node:    n.name,
						term:    n.term,
						success: false,
					}
				}
			}

			if vote.kind == "response" {
				switch vote.success {
				case true:
					// you only want to count votes that are in the same term as your candidate
					// counting old term responses could lead to multiple leaders
					if vote.term == n.term {
						n.votes++

						// majority check : votes > len(peers)/2
						if n.votes > len(n.peers)/2 {
							if n.kind == "candidate" {
								fmt.Println(n.name, " becomes a leader")
								n.kind = "leader"

								//you can start the append ticker here
								appendTicker.Reset(1000 * time.Millisecond)

								// stop election ticker
								electionTicker.Stop()
							}
						}
					}
				case false:
					if vote.term > n.term {
						n.kind = "follower"
						n.term = vote.term
						n.voteFor = ""
					}

				}
			}
		case <-appendTicker.C:
			// we broadcast append req to the other nodes
			for _, peer := range n.peers {
				peer.appendCh <- Append{
					kind: "request",
					node: n.name,
					term: n.term,
				}
			}
		case ap := <-n.appendCh:
			if ap.kind == "request" {
				// if term is larger, adopt the term
				leader := n.peers[ap.node]
				if ap.term >= n.term {

					// steps down if a candidate
					n.kind = "follower"

					// resets the election ticker everytime we get a valid append request
					electionTicker.Reset(3000 * time.Millisecond)

					n.term = ap.term

					// send appendResp back to leader
					leader.appendCh <- Append{
						kind:    "response",
						node:    n.name,
						term:    n.term,
						success: true,
					}
				}

				//if term is smaller, send back a success false
				if ap.term < n.term {
					leader.appendCh <- Append{
						kind:    "response",
						node:    n.name,
						term:    n.term,
						success: false,
					}
				}

			}

			if ap.kind == "response" {

				if ap.success {
					fmt.Println("append resp from: ", ap.node)
				}
				if !ap.success {
					// downgrade node to follower
					// also reset its election ticker
					fmt.Println("downgrading leader to follower: ", n.name)
					n.kind = "follower"

					electionTicker.Reset(1600 * time.Millisecond)
				}
			}

		}
	}

}

func main() {
	var wg sync.WaitGroup

	nodeA := Node{
		name:     "A",
		kind:     "follower",
		term:     0,
		voteFor:  "",
		voteCh:   make(chan Vote, 10),
		appendCh: make(chan Append, 10),
		peers:    make(map[string]Node, 0),
	}

	nodeB := Node{
		name:     "B",
		kind:     "follower",
		term:     0,
		voteFor:  "",
		voteCh:   make(chan Vote, 10),
		appendCh: make(chan Append, 10),
		peers:    make(map[string]Node, 0),
	}

	nodeC := Node{
		name:     "C",
		kind:     "follower",
		term:     0,
		voteFor:  "",
		voteCh:   make(chan Vote, 10),
		appendCh: make(chan Append, 10),
		peers:    make(map[string]Node, 0),
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
