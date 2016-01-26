package paxos

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cmu440-F15/paxosapp/rpc/paxosrpc"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"
)

type signalChan chan struct{}

// Store key value pair
type Storage struct {
	storage     map[string]interface{}
	storageLock *sync.Mutex
}

// Store the highest proposal number for each key
type ProposalNum struct {
	propNum     map[string]int
	propNumLock *sync.Mutex
}

// Store the accepted value and number for each key
type AcceptValues struct {
	acceptValue   map[string]interface{}
	acceptNum     map[string]int
	acceptMapLock *sync.Mutex
}

// Store Connection for each paxos node
type ConnectionList struct {
	conn     map[int]*rpc.Client
	connLock *sync.Mutex
}

// Proposer information
type LeadProposalArgs struct {
	lock       *sync.Mutex
	value      interface{}
	highestNum int
	key        string
}

type paxosNode struct {
	// Node information
	nodePropNum  int
	propNumLock  *sync.Mutex
	numNodes     int
	srvID        int
	majority_num int
	nodeHostPort string
	listener     net.Listener
	hostMap      map[int]string
	// Store values for each key
	storage *Storage
	// Store highest propose number for each key
	propNum *ProposalNum
	// Record the value the node accepted
	acceptValue *AcceptValues
	// Connection for each node
	connList *ConnectionList
}

// NewPaxosNode creates a new PaxosNode. This function should return only when
// all nodes have joined the ring, and should return a non-nil error if the node
// could not be started in spite of dialing the other nodes numRetries times.
//
// hostMap is a map from node IDs to their hostports, numNodes is the number
// of nodes in the ring, replace is a flag which indicates whether this node
// is a replacement for a node which failed.
func NewPaxosNode(myHostPort string, hostMap map[int]string, numNodes, srvId, numRetries int, replace bool) (PaxosNode, error) {
	fmt.Println("NewPaxosNode")
	pn := &paxosNode{}
	pn.nodePropNum = srvId
	pn.propNumLock = new(sync.Mutex)
	pn.numNodes = numNodes
	pn.srvID = srvId
	pn.majority_num = numNodes / 2
	pn.nodeHostPort = myHostPort
	pn.hostMap = hostMap

	pn.storage = &Storage{}
	pn.storage.storage = make(map[string]interface{})
	pn.storage.storageLock = new(sync.Mutex)

	pn.propNum = &ProposalNum{}
	pn.propNum.propNum = make(map[string]int)
	pn.propNum.propNumLock = new(sync.Mutex)

	pn.acceptValue = &AcceptValues{}
	pn.acceptValue.acceptValue = make(map[string]interface{})
	pn.acceptValue.acceptNum = make(map[string]int)
	pn.acceptValue.acceptMapLock = new(sync.Mutex)

	pn.connList = &ConnectionList{}
	pn.connList.conn = make(map[int]*rpc.Client)
	pn.connList.connLock = new(sync.Mutex)

	ln, err := net.Listen("tcp", myHostPort)
	if err != nil {
		fmt.Println("Error on listern: ", err)
		return nil, errors.New("Error on listen: " + err.Error())
	}

	// Begin to listen for incoming connections
	rpc.RegisterName("PaxosNode", paxosrpc.Wrap(pn))
	rpc.HandleHTTP()

	pn.listener = ln
	go http.Serve(ln, nil)

	retryCounter := 0
	// Connect with other servers
	pn.connList.connLock.Lock()
	for len(pn.connList.conn) < pn.numNodes {
		for i, host := range pn.hostMap {
			if _, ok := pn.connList.conn[i]; !ok {
				client, err := rpc.DialHTTP("tcp", host)
				if err == nil {
					pn.connList.conn[i] = client
				}
				// If the node is to replace the dead node
				// It needs to call rpc RecvReplaceServer function to other nodes
				if replace && i != srvId {
					replaceArgs := &paxosrpc.ReplaceServerArgs{SrvID: pn.srvID, Hostport: pn.nodeHostPort}
					var replaceReply paxosrpc.ReplaceServerReply
					client.Call("PaxosNode.RecvReplaceServer", replaceArgs, &replaceReply)
				}
			}
		}
		if len(pn.connList.conn) < pn.numNodes {
			time.Sleep(time.Duration(1) * time.Second)
			retryCounter += 1
			if retryCounter > numRetries {
				return nil, errors.New("Cannot connect with other nodes")
			}
		}
	}
	// If the node is to replace the dead node
	// It needs to get all commited information from an arbitrary node
	if replace {
		pn.storage.storageLock.Lock()
		for id, conn := range pn.connList.conn {
			if id == pn.srvID {
				continue
			}
			var replaceCatchupArgs paxosrpc.ReplaceCatchupArgs
			var replaceReply paxosrpc.ReplaceCatchupReply
			conn.Call("PaxosNode.RecvReplaceCatchup", replaceCatchupArgs, &replaceReply)
			err := json.Unmarshal(replaceReply.Data, &pn.storage.storage)
			if err != nil {
				fmt.Println("[ERROR]", err)
			}
			break
		}
		pn.storage.storageLock.Unlock()
	}
	pn.connList.connLock.Unlock()
	fmt.Println("Paxos node started on port " + myHostPort)
	return pn, nil
}

func (pn *paxosNode) GetNextProposalNumber(args *paxosrpc.ProposalNumberArgs, reply *paxosrpc.ProposalNumberReply) error {
	pn.propNumLock.Lock()
	defer pn.propNumLock.Unlock()

	reply.N = pn.nodePropNum
	pn.nodePropNum += pn.numNodes
	return nil
}

func (pn *paxosNode) Propose(args *paxosrpc.ProposeArgs, reply *paxosrpc.ProposeReply) error {
	timeOutChan := time.After(time.Duration(15) * time.Second)
	proposeSuccess := make(signalChan, pn.numNodes)
	acceptSuccess := make(signalChan, pn.numNodes)

	// Set arguments of proposal
	proposalArgs := &LeadProposalArgs{}
	proposalArgs.value = args.V
	proposalArgs.highestNum = 0
	proposalArgs.lock = new(sync.Mutex)

	// Send prepare message to each node
	pn.connList.connLock.Lock()
	for _, conn := range pn.connList.conn {
		client := conn
		go func() {
			prepareArgs := &paxosrpc.PrepareArgs{Key: args.Key, N: args.N}
			var prepare_reply paxosrpc.PrepareReply
			client.Call("PaxosNode.RecvPrepare", prepareArgs, &prepare_reply)
			if prepare_reply.Status == paxosrpc.OK {
				proposeSuccess <- struct{}{}
				// Pick the accepted value with highest seq num
				proposalArgs.lock.Lock()
				if prepare_reply.V_a != nil && prepare_reply.N_a > proposalArgs.highestNum {
					// fmt.Printf("[PROPOSE] [ID %d] N_a: %d, V_a %d\n", pn.srvID, prepare_reply.N_a, prepare_reply.V_a)
					proposalArgs.value = prepare_reply.V_a
					proposalArgs.highestNum = prepare_reply.N_a
				}
				proposalArgs.lock.Unlock()
			}
		}()
	}
	pn.connList.connLock.Unlock()

	// Count the number of accept messages
	// If the number exceeds the half of the number of nodes
	// Move to ACCEPT Phase
	propose_counter := 0
Propose:
	for {
		select {
		case <-timeOutChan:
			fmt.Println("[Timeout]")
			reply.V = nil
			return nil
		case <-proposeSuccess:
			propose_counter += 1
			if propose_counter > pn.majority_num {
				break Propose
			}
		}
	}

	// Send ACCEPT message to each node
	pn.connList.connLock.Lock()
	for _, conn := range pn.connList.conn {
		client := conn
		go func() {
			accept_args := &paxosrpc.AcceptArgs{Key: args.Key, N: args.N, V: proposalArgs.value}
			var acc_reply paxosrpc.AcceptReply
			client.Call("PaxosNode.RecvAccept", accept_args, &acc_reply)
			if acc_reply.Status == paxosrpc.OK {
				acceptSuccess <- struct{}{}
			}
		}()
	}
	pn.connList.connLock.Unlock()

	// Count the number of accept messages
	// If the number exceeds the half of the number of nodes
	// Move to COMMIT Phase
	accept_counter := 0
Accept:
	for {
		select {
		case <-timeOutChan:
			fmt.Println("[Timeout]")
			reply.V = nil
			return nil
		case <-acceptSuccess:
			accept_counter += 1
			if accept_counter > pn.majority_num {
				break Accept
			}
		}
	}

	// Send COMMIT message to each node
	pn.connList.connLock.Lock()
	for _, conn := range pn.connList.conn {
		client := conn
		replyChan := make(chan bool)
		go func() {
			commit_args := &paxosrpc.CommitArgs{Key: args.Key, V: proposalArgs.value}
			var commit_reply paxosrpc.CommitReply
			client.Call("PaxosNode.RecvCommit", commit_args, &commit_reply)
			replyChan <- true
		}()
		select {
		case <-replyChan:
			break
		case <-time.After(time.Duration(1) * time.Second):
			break
		}
	}
	pn.connList.connLock.Unlock()
	reply.V = proposalArgs.value
	return nil
}

// Self-defined function
// It will return all commited information
func (pn *paxosNode) GetMap(args *paxosrpc.GetMapArgs, reply *paxosrpc.GetMapReply) error {
	pn.storage.storageLock.Lock()
	defer pn.storage.storageLock.Unlock()
	jsonMap, _ := json.Marshal(pn.storage.storage)
	reply.Data = jsonMap
	return nil
}

// It will check whether the key is in the storage
// If so, return the value and set status as KeyFound
// If not, set status as Key Not Found
func (pn *paxosNode) GetValue(args *paxosrpc.GetValueArgs, reply *paxosrpc.GetValueReply) error {
	recvKey := args.Key
	pn.storage.storageLock.Lock()
	defer pn.storage.storageLock.Unlock()
	if value, ok := pn.storage.storage[recvKey]; ok {
		reply.Status = paxosrpc.KeyFound
		reply.V = value
	} else {
		reply.Status = paxosrpc.KeyNotFound
		fmt.Println("[GETVALUE] Key Not Found")
	}
	return nil
}

// It will check whether the key is in the storage
// If so, return the value and set status as KeyFound
// If not, set status as Key Not Found
func (pn *paxosNode) RecvPrepare(args *paxosrpc.PrepareArgs, reply *paxosrpc.PrepareReply) error {
	recvKey := args.Key
	recvNum := args.N

	pn.propNum.propNumLock.Lock()
	defer pn.propNum.propNumLock.Unlock()

	if propNum, ok := pn.propNum.propNum[recvKey]; ok {
		if recvNum < propNum {
			reply.Status = paxosrpc.Reject
			reply.N_a = -1
		} else {
			reply.Status = paxosrpc.OK
			pn.propNum.propNum[recvKey] = recvNum
			pn.acceptValue.acceptMapLock.Lock()
			if acc_v, ok := pn.acceptValue.acceptValue[recvKey]; ok {
				reply.V_a = acc_v
				reply.N_a = pn.acceptValue.acceptNum[recvKey]
			} else {
				reply.V_a = nil
				reply.N_a = -1
			}
			pn.acceptValue.acceptMapLock.Unlock()
		}
	} else {
		pn.propNum.propNum[recvKey] = recvNum
		reply.Status = paxosrpc.OK
		reply.N_a = -1
	}
	return nil
}

// When a node receive an accept message
// It will compare the highest number it has seen with the number in the message
// If the number in the message is higher, accept the message and update the information
// If not, set status as reject
func (pn *paxosNode) RecvAccept(args *paxosrpc.AcceptArgs, reply *paxosrpc.AcceptReply) error {
	recvKey := args.Key
	recvNum := args.N

	pn.propNum.propNumLock.Lock()
	pn.acceptValue.acceptMapLock.Lock()
	defer pn.acceptValue.acceptMapLock.Unlock()
	defer pn.propNum.propNumLock.Unlock()

	if propNum, ok := pn.propNum.propNum[recvKey]; ok {
		if recvNum < propNum {
			reply.Status = paxosrpc.Reject
		} else {
			pn.propNum.propNum[recvKey] = recvNum
			pn.acceptValue.acceptValue[recvKey] = args.V
			pn.acceptValue.acceptNum[recvKey] = args.N
			reply.Status = paxosrpc.OK
		}
	} else {
		pn.propNum.propNum[recvKey] = recvNum
		pn.acceptValue.acceptValue[recvKey] = args.V
		pn.acceptValue.acceptNum[recvKey] = args.N
		reply.Status = paxosrpc.OK
	}
	return nil
}

// When a node receive a COMMIT message
// Store the value into storage
func (pn *paxosNode) RecvCommit(args *paxosrpc.CommitArgs, reply *paxosrpc.CommitReply) error {
	recvKey := args.Key

	pn.storage.storageLock.Lock()
	pn.acceptValue.acceptMapLock.Lock()
	pn.propNum.propNumLock.Lock()
	defer pn.propNum.propNumLock.Unlock()
	defer pn.acceptValue.acceptMapLock.Unlock()
	defer pn.storage.storageLock.Unlock()

	delete(pn.acceptValue.acceptValue, recvKey)
	delete(pn.propNum.propNum, recvKey)
	pn.storage.storage[recvKey] = args.V
	return nil
}

// Replace the dead node with new node's information
func (pn *paxosNode) RecvReplaceServer(args *paxosrpc.ReplaceServerArgs, reply *paxosrpc.ReplaceServerReply) error {
	pn.connList.connLock.Lock()
	defer pn.connList.connLock.Unlock()
	client, err := rpc.DialHTTP("tcp", args.Hostport)
	if err == nil {
		fmt.Printf("Replace node %d successfully [from node %d]\n", args.SrvID, pn.srvID)
		pn.connList.conn[args.SrvID] = client
	}
	return nil
}

// Send all commited information to new node
func (pn *paxosNode) RecvReplaceCatchup(args *paxosrpc.ReplaceCatchupArgs, reply *paxosrpc.ReplaceCatchupReply) error {
	pn.storage.storageLock.Lock()
	defer pn.storage.storageLock.Unlock()
	jsonMap, _ := json.Marshal(pn.storage.storage)
	reply.Data = jsonMap
	return nil
}
