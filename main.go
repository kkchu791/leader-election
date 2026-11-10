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
}

func process(n Node, in chan Vote, out chan Vote, ticker *time.Ticker) {
	for {
		select {
		case <-ticker.C:
			fmt.Println("election timeout ", n.name)
			// becomeing a candidate
			n.kind = "Candidate"

			// updates the term
			n.term++

			// votes for itself
			n.votes++

			//sending to all other nodes VoteReq
			out <- Vote{
				kind: "request",
				node: n.name,
				term: n.term,
			}
		case vote := <-in:

			if vote.kind == "request" {
				//this is getting a requestVote and sending out a responseVote
				if n.voteFor == "" && vote.term > n.term {

					// update the node voteFor so it can't vote again during this term
					n.voteFor = vote.node

					// update this nodes term
					n.term = vote.term

					// send voteResp
					out <- Vote{
						kind:    "response",
						node:    n.name,
						term:    n.term,
						success: true,
					}
				}
			}

			if vote.kind == "response" {
				fmt.Println("do we ever get voteResp?")
				// majority check : votes > len(peers)/2
				if vote.success == true {
					n.votes++
				}

				if n.votes > 1 {
					fmt.Println(n.name, " becomes a leader")
					n.kind = "Leader"
				}
			}
		}
	}

}

func main() {

	aCh := make(chan Vote)
	bCh := make(chan Vote)

	var wg sync.WaitGroup
	wg.Go(func() {
		tickerA := time.NewTicker(1600 * time.Millisecond)
		node := Node{name: "A", kind: "Follower", term: 0, voteFor: ""}
		process(node, aCh, bCh, tickerA)
	})
	wg.Go(func() {
		tickerB := time.NewTicker(3000 * time.Millisecond)
		node := Node{name: "B", kind: "Follower", term: 0, voteFor: ""}
		process(node, bCh, aCh, tickerB)
	})

	wg.Wait()
}
