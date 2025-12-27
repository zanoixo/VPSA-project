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
}

type ServerDataBase struct {
	users     map[string]int64
	userIndex int64
	userLock  sync.Mutex

	topics     map[string]int64
	topicIndex int64
	topicLock  sync.Mutex

	topicsPosts map[int64]map[int64]db.Message
	postIndex   int64
	postLock    sync.Mutex
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

		server.CRUDServer.userLock.Lock()

		server.CRUDServer.users[req.Name] = server.CRUDServer.userIndex
		currUser.Id = server.CRUDServer.userIndex
		server.CRUDServer.userIndex++

		server.CRUDServer.userLock.Unlock()

	}

	return &currUser, nil
}

func (server *Server) CreateTopic(ctx context.Context, req *db.CreateTopicRequest) (*db.Topic, error) {

	fmt.Printf("[INFO]: Recieved create topic request: %s\n", req.Name)

	newTopic := db.Topic{}
	newTopic.Name = req.Name

	topicId, topicExists := server.CRUDServer.topics[req.Name]

	if topicExists {

		fmt.Printf("[INFO]: Topic exists returning topic ID %d\n", topicId)
		newTopic.Id = topicId
	} else {

		fmt.Printf("[INFO]: Creating topic with ID %d\n", server.CRUDServer.topicIndex)

		server.CRUDServer.topicLock.Lock()

		server.CRUDServer.topics[req.Name] = server.CRUDServer.topicIndex
		server.CRUDServer.topicsPosts[server.CRUDServer.topicIndex] = make(map[int64]db.Message)
		newTopic.Id = server.CRUDServer.topicIndex
		server.CRUDServer.topicIndex++

		server.CRUDServer.topicLock.Unlock()

	}

	return &newTopic, nil
}

func (server *Server) PostMessage(ctx context.Context, req *db.PostMessageRequest) (*db.Message, error) {

	fmt.Printf("[INFO]: Recieved create post request\n")

	newPost := db.Message{}
	newPost.Text = req.Text
	newPost.UserId = req.UserId
	newPost.TopicId = req.TopicId
	newPost.Likes = 0

	server.CRUDServer.postLock.Lock()

	newPost.Id = server.CRUDServer.postIndex
	server.CRUDServer.topicsPosts[req.TopicId][server.CRUDServer.postIndex] = newPost

	server.CRUDServer.postIndex++

	server.CRUDServer.postLock.Unlock()

	return &newPost, nil
}

func (server *Server) LikeMessage(ctx context.Context, req *db.LikeMessageRequest) (*db.Message, error) {
	return nil, status.Error(codes.Unimplemented, "method LikeMessage not implemented")
}

func (server *Server) GetSubscriptionNode(ctx context.Context, req *db.SubscriptionNodeRequest) (*db.SubscriptionNodeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method GetSubscriptionNode not implemented")
}

func (server *Server) ListTopics(ctx context.Context, req *emptypb.Empty) (*db.ListTopicsResponse, error) {

	fmt.Printf("[INFO]: Recieved list topics request\n")

	topicList := db.ListTopicsResponse{}
	topicList.Topics = make([]*db.Topic, 0, len(server.CRUDServer.topics))

	for key, val := range server.CRUDServer.topics {
		topicList.Topics = append(topicList.Topics, &db.Topic{Id: val, Name: key})
	}

	return &topicList, nil
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

	CRUD.topics = make(map[string]int64)
	CRUD.topicIndex = 0

	CRUD.topicsPosts = make(map[int64]map[int64]db.Message)
	CRUD.postIndex = 0

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
