package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"sync"

	db "github.com/zanoixo/VPSA-project/razpravljalnica"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Server struct {
	db.UnimplementedMessageBoardServer
	CRUDServer *ServerDataBase
	dbLock     sync.Mutex
}

type ServerDataBase struct {
	users     map[string]int64
	userIndex int64
}

func (server *Server) CreateUser(ctx context.Context, req *db.CreateUserRequest) (*db.User, error) {

	fmt.Printf("[INFO]: Recieved create user request from %s\n", req.Name)

	currUser := db.User{}
	currUser.Name = req.Name

	userId, userExists := server.CRUDServer.users[req.Name]

	if userExists {

		fmt.Printf("[INFO]: User exists returning user ID %d\n", userId)
		currUser.Id = userId
	} else {

		fmt.Printf("[INFO]: Creating user with ID %d\n", server.CRUDServer.userIndex)

		server.dbLock.Lock()

		server.CRUDServer.users[req.Name] = server.CRUDServer.userIndex
		currUser.Id = server.CRUDServer.userIndex
		server.CRUDServer.userIndex++

		server.dbLock.Unlock()

	}

	return &currUser, nil
}

func (server *Server) CreateTopic(ctx context.Context, req *db.CreateTopicRequest) (*db.Topic, error) {
	return nil, status.Error(codes.Unimplemented, "method CreateTopic not implemented")
}

func (server *Server) PostMessage(ctx context.Context, req *db.PostMessageRequest) (*db.Message, error) {
	return nil, status.Error(codes.Unimplemented, "method PostMessage not implemented")
}

func (server *Server) LikeMessage(ctx context.Context, req *db.LikeMessageRequest) (*db.Message, error) {
	return nil, status.Error(codes.Unimplemented, "method LikeMessage not implemented")
}

func (server *Server) GetSubscriptionNode(ctx context.Context, req *db.SubscriptionNodeRequest) (*db.SubscriptionNodeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method GetSubscriptionNode not implemented")
}

func (server *Server) ListTopics(ctx context.Context, req *emptypb.Empty) (*db.ListTopicsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method ListTopics not implemented")
}

func (server *Server) GetMessages(ctx context.Context, req *db.GetMessagesRequest) (*db.GetMessagesResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method GetMessages not implemented")
}

func (server *Server) SubscribeTopic(req *db.SubscribeTopicRequest, stream db.MessageBoard_SubscribeTopicServer) error {
	return status.Error(codes.Unimplemented, "method SubscribeTopic not implemented")
}

func checkError(err error) {

	if err != nil {
		panic(fmt.Sprintf("[ERROR]: %s", err))
	}

}

func startServer(url string) {

	CRUD := ServerDataBase{}
	CRUD.users = make(map[string]int64)
	CRUD.userIndex = 0

	fmt.Printf("Server starting: %s\n", url)
	grpcServer := grpc.NewServer()

	server := Server{}
	server.CRUDServer = &CRUD

	db.RegisterMessageBoardServer(grpcServer, &server)

	listener, err := net.Listen("tcp", url)
	checkError(err)

	err = grpcServer.Serve(listener)
	checkError(err)

}

func main() {

	iPtr := flag.String("ip", "localhost", "server IP")
	pPtr := flag.Int("p", 6000, "server port")
	flag.Parse()

	url := fmt.Sprintf("%v:%v", *iPtr, *pPtr)

	startServer(url)
}
