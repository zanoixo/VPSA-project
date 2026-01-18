package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"sync"
	"time"

	db "github.com/zanoixo/VPSA-project/razpravljalnica"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type workerServer struct {
	url           string
	nodeId        string
	prevServer    *workerServer
	nextServer    *workerServer
	ControlClient db.ControlPlaneClient
}

type ControlServer struct {
	db.UnimplementedControlPlaneServer
	ServerChain    *workerServer
	ServersAlive   map[string]int
	chainLock      sync.Mutex
	numOfServers   int
	nextSubServer  int
	msgBoardClient db.MessageBoardClient
}

func checkError(err error) {

	if err != nil {
		panic(fmt.Sprintf("[ERROR]: %s", err))
	}

}

func (controlServer *ControlServer) GetSubscriptionNode(ctx context.Context, req *db.SubscriptionNodeRequest) (*db.SubscriptionNodeResponse, error) {

	fmt.Printf("[INFO]: recieved getSubscription request\n")

	subNode := &db.NodeInfo{}

	for {

		currServer := controlServer.ServerChain

		counter := 0

		fmt.Printf("%d %d\n", controlServer.nextSubServer, controlServer.numOfServers)

		for counter != controlServer.nextSubServer {

			currServer = currServer.nextServer
			counter++
		}

		nextSubConn, _ := grpc.NewClient(currServer.url, grpc.WithTransportCredentials(insecure.NewCredentials()))
		controlServer.msgBoardClient = db.NewMessageBoardClient(nextSubConn)

		_, err := controlServer.msgBoardClient.Ping(context.Background(), &emptypb.Empty{})

		controlServer.nextSubServer = (controlServer.nextSubServer + 1) % controlServer.numOfServers
		if err == nil {

			break
		}

		fmt.Printf("[INFO] can't connect to %s\n", subNode.Address)

	}

	nodeResp, err := controlServer.msgBoardClient.GetSubscription(ctx, req)

	return nodeResp, err

}

func (ControlServer *ControlServer) GetTail() *workerServer {

	currServer := ControlServer.ServerChain

	if currServer == nil {

		return nil
	}

	for currServer.nextServer != nil {

		currServer = currServer.nextServer
	}

	return currServer
}

func (ControlServer *ControlServer) GetHead() *workerServer {

	return ControlServer.ServerChain
}

func (ControlServer *ControlServer) GetClusterState(ctx context.Context, _ *emptypb.Empty) (*db.GetClusterStateResponse, error) {

	fmt.Printf("[INFO]: New cluster state request recieved\n")

	head := ControlServer.GetHead()
	tail := ControlServer.GetTail()

	clusterStateRes := db.GetClusterStateResponse{}

	if head == nil || tail == nil {

		return nil, status.Error(codes.Unavailable, "Curently no servers are up an running")
	}

	clusterStateRes.Head = &db.NodeInfo{Address: head.url, NodeId: head.nodeId}
	clusterStateRes.Tail = &db.NodeInfo{Address: tail.url, NodeId: tail.nodeId}

	return &clusterStateRes, nil
}

func (ControlServer *ControlServer) RemoveServer(nodeId string) {

	currServer := ControlServer.ServerChain

	for currServer.nodeId != nodeId {

		currServer = currServer.nextServer
	}

	if currServer.prevServer != nil {

		currServer.prevServer.nextServer = currServer.nextServer

		newNextServer := &db.NodeInfo{}

		if currServer.nextServer == nil {

			newNextServer.Address = ""
			newNextServer.NodeId = ""

		} else {
			newNextServer.Address = currServer.nextServer.url
			newNextServer.NodeId = currServer.nextServer.nodeId
		}

		currServer.prevServer.ControlClient.SetNextServer(context.Background(), &db.NextServerRequest{NextServer: newNextServer})

	} else {

		fmt.Printf("[INFO]: New head request sent to server %s\n", currServer.nextServer.url)
		currServer.nextServer.ControlClient.SetHead(context.Background(), &db.SetHeadRequest{Head: true})
		ControlServer.ServerChain = currServer.nextServer
	}

	if currServer.nextServer != nil {

		currServer.nextServer.prevServer = currServer.prevServer

	} else {

		fmt.Printf("[INFO]: New tail request sent to server %s\n", currServer.prevServer.url)
		currServer.prevServer.ControlClient.SetTail(context.Background(), &db.SetTailRequest{Tail: true})
	}

	fmt.Printf("[INFO]: Removing server %s\n", currServer.url)

}

func (ControlServer *ControlServer) CheckWorkServers() {

	for {

		ControlServer.chainLock.Lock()

		currServer := ControlServer.ServerChain

		if currServer == nil {

			ControlServer.chainLock.Unlock()
			time.Sleep(500 * time.Millisecond)
			continue
		}

		for currServer != nil {

			_, err := currServer.ControlClient.ControlPing(context.Background(), &emptypb.Empty{})

			if err != nil {

				ControlServer.ServersAlive[currServer.nodeId] += 1
			} else {

				ControlServer.ServersAlive[currServer.nodeId] = 0
			}

			if ControlServer.ServersAlive[currServer.nodeId] >= 3 {

				fmt.Printf("[INFO]: Server unresponsive removing: %s\n", currServer.url)
				ControlServer.numOfServers--
				ControlServer.RemoveServer(currServer.nodeId)

			}

			currServer = currServer.nextServer
		}

		ControlServer.chainLock.Unlock()
		time.Sleep(500 * time.Millisecond)
	}
}

func (ControlServer *ControlServer) NewServer(ctx context.Context, req *db.NewServerRequest) (*emptypb.Empty, error) {

	ControlServer.chainLock.Lock()

	currServer := ControlServer.ServerChain

	newConn, err := grpc.NewClient(req.NewServer.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))

	newClient := db.NewControlPlaneClient(newConn)

	checkError(err) //replace with retry logic

	fmt.Printf("[INFO]: adding new server %s\n", req.NewServer.Address)

	if currServer == nil {

		ControlServer.ServerChain = &workerServer{url: req.NewServer.Address, nodeId: req.NewServer.NodeId, prevServer: nil, nextServer: nil, ControlClient: newClient}

		newClient.SetTail(context.Background(), &db.SetTailRequest{Tail: true})

		newClient.SetHead(context.Background(), &db.SetHeadRequest{Head: true})

		fmt.Printf("[INFO]: New tail request sent to server %s\n", req.NewServer.Address)
		fmt.Printf("[INFO]: New head request sent to server %s\n", req.NewServer.Address)

	} else {

		tail := ControlServer.GetTail()

		tail.nextServer = &workerServer{url: req.NewServer.Address, nodeId: req.NewServer.NodeId, prevServer: tail, nextServer: nil, ControlClient: newClient}

		tail.ControlClient.SetTail(context.Background(), &db.SetTailRequest{Tail: false})

		fmt.Printf("[INFO]: Tail removal request sent to server %s\n", tail.url)

		newClient.SetTail(context.Background(), &db.SetTailRequest{Tail: true})

		fmt.Printf("[INFO]: New tail request sent to server %s\n", req.NewServer.Address)

		tail.ControlClient.SetNextServer(context.Background(), &db.NextServerRequest{NextServer: req.NewServer})
	}

	ControlServer.ServersAlive[req.NewServer.NodeId] = 0
	ControlServer.numOfServers++

	ControlServer.chainLock.Unlock()

	return &emptypb.Empty{}, nil
}

func startServer(url string) {

	fmt.Printf("Server control starting: %s\n", url)
	grpcServer := grpc.NewServer()

	controlServer := ControlServer{}
	controlServer.ServersAlive = make(map[string]int)
	controlServer.nextSubServer = 0
	controlServer.numOfServers = 0

	db.RegisterControlPlaneServer(grpcServer, &controlServer)

	go controlServer.CheckWorkServers()

	listener, err := net.Listen("tcp", url)
	checkError(err)

	err = grpcServer.Serve(listener)
	checkError(err)

}

func main() {

	iPtr := flag.String("ip", "localhost", "server IP")
	pPtr := flag.Int("p", 6000, "server port")

	flag.Parse()

	ip := *iPtr
	port := *pPtr

	url := fmt.Sprintf("%v:%v", ip, port)

	startServer(url)
}
