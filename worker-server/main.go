package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"net"
	"slices"
	"sort"
	"sync"
	"time"

	db "github.com/zanoixo/VPSA-project/razpravljalnica"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type SubscriptionData struct {
	topic        int64
	fromMsgIndex int64
	lastMsgIndex int64
}

type Server struct {
	db.UnimplementedMessageBoardServer
	db.UnimplementedSyncDataServer
	CRUDServer      *ServerDataBase
	syncDataService db.SyncDataClient
	msgBoardSubGen  db.MessageBoardClient

	url    string
	nodeId string

	isHead        bool
	isTail        bool
	nextServerUrl string

	numOfServer int

	nodes       []*db.NodeInfo
	nextSub     int
	subNodeLock sync.Mutex
}

type ServerDataBase struct {
	users     map[string]int64 //map[username]
	userIndex int64
	userLock  sync.Mutex

	topics     map[string]int64 //map[nameOfTopic]
	topicIndex int64
	topicLock  sync.Mutex

	topicsPosts     map[int64]map[int64]*db.Message //map[topicId][postId]
	topicsPostsList map[int64][]*db.Message         //optimization for returning all posts in a single topic
	postIndex       int64
	postLock        sync.Mutex

	userLikes map[int64][]int64 //map[userId]array[messegeIds]
	likesLock sync.Mutex

	userSubscription     map[string][]*SubscriptionData
	userSubscriptionLock sync.Mutex
}

func (server *Server) Ping(ctx context.Context, req *emptypb.Empty) (*emptypb.Empty, error) {

	return &emptypb.Empty{}, nil
}

func (server *Server) SyncPing(ctx context.Context, req *emptypb.Empty) (*emptypb.Empty, error) {

	return &emptypb.Empty{}, nil
}

func (server *Server) SyncUser(ctx context.Context, req *db.SyncUserRequest) (*emptypb.Empty, error) {

	fmt.Printf("[INFO]: Syncing create user request from %s with id: %d\n", req.Name, req.Id)

	server.CRUDServer.userLock.Lock()

	server.CRUDServer.users[req.Name] = req.Id
	server.CRUDServer.userIndex++

	server.CRUDServer.userLock.Unlock()

	if !server.isTail {

		server.syncDataService.SyncUser(context.Background(), req)
	}

	return nil, nil

}

func (server *Server) SyncTopic(ctx context.Context, req *db.SyncTopicRequest) (*emptypb.Empty, error) {

	fmt.Printf("[INFO]: Recieved sync topic request: %s with id %d\n", req.Name, req.Id)

	server.CRUDServer.topicLock.Lock()

	server.CRUDServer.topics[req.Name] = req.Id
	server.CRUDServer.topicIndex++

	server.CRUDServer.topicLock.Unlock()

	server.CRUDServer.postLock.Lock()

	server.CRUDServer.topicsPosts[req.Id] = make(map[int64]*db.Message)
	server.CRUDServer.topicsPostsList[req.Id] = []*db.Message{}

	server.CRUDServer.postLock.Unlock()

	if !server.isTail {

		server.syncDataService.SyncTopic(context.Background(), req)
	}

	return nil, nil
}

func (server *Server) SyncLike(ctx context.Context, req *db.SyncLikeRequest) (*emptypb.Empty, error) {

	fmt.Printf("[INFO]: Recieved sync like request: with user id: %d topicId: %d msgId: %d\n", req.UserId, req.TopicId, req.MessageId)

	server.CRUDServer.likesLock.Lock()

	server.CRUDServer.userLikes[req.UserId] = append(server.CRUDServer.userLikes[req.UserId], req.MessageId)

	server.CRUDServer.likesLock.Unlock()

	server.CRUDServer.postLock.Lock()

	msg, err := server.CRUDServer.topicsPosts[req.TopicId][req.MessageId]

	if !err && msg != nil {
		server.CRUDServer.topicsPosts[req.TopicId][req.MessageId].Likes++

	} else {

		server.CRUDServer.postLock.Unlock()
		return nil, status.Error(codes.NotFound, "Post doesnt exist")

	}

	server.CRUDServer.postLock.Unlock()

	if !server.isTail {

		server.syncDataService.SyncLike(context.Background(), req)
	}

	return nil, nil
}

func (server *Server) SyncMessage(ctx context.Context, req *db.SyncMessageRequest) (*emptypb.Empty, error) {

	fmt.Printf("[INFO]: Recieved sync post request with topicId: %d postId: %d\n", req.TopicId, req.PostId)

	server.CRUDServer.postLock.Lock()

	server.CRUDServer.topicsPosts[req.TopicId][req.PostId] = req.Post
	server.CRUDServer.topicsPostsList[req.TopicId] = append(server.CRUDServer.topicsPostsList[req.TopicId], req.Post)
	server.CRUDServer.postIndex++

	server.CRUDServer.postLock.Unlock()

	if !server.isTail {

		server.syncDataService.SyncMessage(context.Background(), req)
	}

	return nil, nil
}

func (server *Server) CreateUser(ctx context.Context, req *db.CreateUserRequest) (*db.User, error) {

	fmt.Printf("[INFO]: Recieved create user request from %s\n", req.Name)

	currUser := db.User{}
	currUser.Name = req.Name

	server.CRUDServer.userLock.Lock()

	userId, userExists := server.CRUDServer.users[req.Name]

	server.CRUDServer.userLock.Unlock()

	if userExists {

		fmt.Printf("[INFO]: User exists returning user ID %d\n", userId)
		currUser.Id = userId

	} else {

		fmt.Printf("[INFO]: Creating user with ID %d\n", server.CRUDServer.userIndex)

		server.CRUDServer.userLock.Lock()

		server.CRUDServer.users[req.Name] = server.CRUDServer.userIndex
		currUser.Id = server.CRUDServer.userIndex
		server.CRUDServer.userIndex++

		server.CRUDServer.userLikes[currUser.Id] = []int64{}

		server.CRUDServer.userLock.Unlock()

	}

	if !server.isTail {

		syncReq := &db.SyncUserRequest{Id: currUser.Id, Name: req.Name}

		_, err := server.syncDataService.SyncUser(context.Background(), syncReq)

		if err != nil {

			return nil, err
		}
	}

	return &currUser, nil
}

func (server *Server) CreateTopic(ctx context.Context, req *db.CreateTopicRequest) (*db.Topic, error) {

	fmt.Printf("[INFO]: Recieved create topic request: %s\n", req.Name)

	newTopic := db.Topic{}
	newTopic.Name = req.Name

	server.CRUDServer.topicLock.Lock()

	topicId, topicExists := server.CRUDServer.topics[req.Name]

	server.CRUDServer.topicLock.Unlock()

	if topicExists {

		fmt.Printf("[INFO]: Topic exists returning topic ID %d\n", topicId)
		return nil, status.Error(codes.AlreadyExists, "Topic already exist")
	} else {

		fmt.Printf("[INFO]: Creating topic with ID %d\n", server.CRUDServer.topicIndex)

		server.CRUDServer.topicLock.Lock()

		server.CRUDServer.topics[req.Name] = server.CRUDServer.topicIndex
		newTopic.Id = server.CRUDServer.topicIndex
		server.CRUDServer.topicIndex++

		server.CRUDServer.topicLock.Unlock()

		server.CRUDServer.postLock.Lock()

		server.CRUDServer.topicsPosts[newTopic.Id] = make(map[int64]*db.Message)
		server.CRUDServer.topicsPostsList[newTopic.Id] = []*db.Message{}

		server.CRUDServer.postLock.Unlock()

	}

	if !server.isTail {

		syncReq := &db.SyncTopicRequest{Id: newTopic.Id, Name: req.Name}

		_, err := server.syncDataService.SyncTopic(context.Background(), syncReq)

		if err != nil {

			return nil, err
		}
	}

	return &newTopic, nil
}

func (server *Server) PostMessage(ctx context.Context, req *db.PostMessageRequest) (*db.Message, error) {

	fmt.Printf("[INFO]: Recieved create post request\n")

	newPost := &db.Message{}
	newPost.Text = req.Text
	newPost.UserId = req.UserId
	newPost.TopicId = req.TopicId
	newPost.Likes = 0

	server.CRUDServer.postLock.Lock()

	_, topicExists := server.CRUDServer.topicsPosts[req.TopicId]

	server.CRUDServer.postLock.Unlock()

	if !topicExists {

		return nil, status.Error(codes.NotFound, "Topic doesnt exist")
	}

	server.CRUDServer.postLock.Lock()

	newPost.Id = server.CRUDServer.postIndex
	server.CRUDServer.topicsPosts[req.TopicId][server.CRUDServer.postIndex] = newPost
	server.CRUDServer.topicsPostsList[req.TopicId] = append(server.CRUDServer.topicsPostsList[req.TopicId], newPost)

	server.CRUDServer.postIndex++

	server.CRUDServer.postLock.Unlock()

	if !server.isTail {

		syncReq := &db.SyncMessageRequest{TopicId: req.TopicId, PostId: newPost.Id, Post: newPost}

		_, err := server.syncDataService.SyncMessage(context.Background(), syncReq)

		if err != nil {

			return nil, err
		}
	}

	return newPost, nil
}

func (server *Server) userExists(userId int64) string {

	user := ""

	server.CRUDServer.userLock.Lock()

	for username, id := range server.CRUDServer.users {

		if id == userId {

			server.CRUDServer.userLock.Unlock()
			return username
		}
	}

	server.CRUDServer.userLock.Unlock()
	return user

}

func (server *Server) alreadyLiked(userId int64, messageId int64) bool {

	server.CRUDServer.likesLock.Lock()

	isLiked := slices.Contains(server.CRUDServer.userLikes[userId], messageId)

	server.CRUDServer.likesLock.Unlock()

	return isLiked
}

func (server *Server) LikeMessage(ctx context.Context, req *db.LikeMessageRequest) (*db.Message, error) {

	fmt.Printf("[INFO]: Like message request recieved\n")

	server.CRUDServer.postLock.Lock()

	likedMsg, msgExists := server.CRUDServer.topicsPosts[req.TopicId][req.MessageId]

	server.CRUDServer.postLock.Unlock()

	if !msgExists {

		fmt.Printf("[INFO]: Post doesnt exist\n")
		return nil, status.Error(codes.NotFound, "Post doesnt exist")
	}

	if server.userExists(req.UserId) == "" {

		fmt.Printf("[INFO]: User doesnt exist\n")
		return nil, status.Error(codes.NotFound, "User doesnt exist")
	}

	if server.alreadyLiked(req.UserId, req.MessageId) {

		fmt.Printf("[INFO]: Post already liked\n")
		return nil, status.Error(codes.NotFound, "Post already liked")
	}

	server.CRUDServer.postLock.Lock()

	server.CRUDServer.topicsPosts[req.TopicId][req.MessageId].Likes++

	server.CRUDServer.postLock.Unlock()

	server.CRUDServer.likesLock.Lock()

	server.CRUDServer.userLikes[req.UserId] = append(server.CRUDServer.userLikes[req.UserId], req.MessageId)

	server.CRUDServer.likesLock.Unlock()

	fmt.Printf("[INFO]: Post liked\n")

	if !server.isTail {

		syncReq := &db.SyncLikeRequest{MessageId: req.MessageId, UserId: req.UserId, TopicId: req.TopicId}

		_, err := server.syncDataService.SyncLike(context.Background(), syncReq)

		if err != nil {

			return nil, err
		}
	}

	return likedMsg, nil
}

func (server *Server) GenerateSubscription(ctx context.Context, req *db.SubscriptionNodeRequest) (*emptypb.Empty, error) {

	user := server.userExists(req.UserId)

	if user == "" {

		fmt.Printf("[INFO]: User doesnt exist\n")
		return nil, status.Error(codes.NotFound, "User doesnt exist")
	}

	userToken := sha256.Sum256([]byte(user))

	subToken := hex.EncodeToString(userToken[:])

	server.CRUDServer.userSubscriptionLock.Lock()

	_, userTokenExists := server.CRUDServer.userSubscription[subToken]

	if !userTokenExists {

		server.CRUDServer.userSubscription[subToken] = []*SubscriptionData{}

		fmt.Printf("[INFO]: generated userToken\n")
	}

	server.CRUDServer.userSubscriptionLock.Unlock()

	return nil, nil
}

func (server *Server) GetSubscriptionNode(ctx context.Context, req *db.SubscriptionNodeRequest) (*db.SubscriptionNodeResponse, error) {

	fmt.Printf("[INFO]: recieved getSubscription request\n")

	if !server.isHead {

		return nil, status.Error(codes.PermissionDenied, "Can't request a node from a server that isnt the head server")
	}

	user := server.userExists(req.UserId)

	if user == "" {

		fmt.Printf("[INFO]: User doesnt exist\n")
		return nil, status.Error(codes.NotFound, "User doesnt exist")
	}

	server.subNodeLock.Lock()

	subNode := &db.NodeInfo{}

	for {

		subNode = server.nodes[server.nextSub]
		server.nextSub = (server.nextSub + 1) % server.numOfServer

		nextSubConn, _ := grpc.NewClient(subNode.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		server.msgBoardSubGen = db.NewMessageBoardClient(nextSubConn)

		_, err := server.msgBoardSubGen.Ping(context.Background(), &emptypb.Empty{})

		if err == nil {

			break
		}

		fmt.Printf("[INFO] can't connect to %s\n", subNode.Address)
	}

	server.subNodeLock.Unlock()

	userToken := sha256.Sum256([]byte(user))

	subToken := hex.EncodeToString(userToken[:])

	_, err := server.msgBoardSubGen.GenerateSubscription(context.Background(), req)

	if err != nil {

		return nil, err
	}

	subResp := &db.SubscriptionNodeResponse{Node: subNode, SubscribeToken: subToken}

	return subResp, nil
}

func (server *Server) ListTopics(ctx context.Context, req *emptypb.Empty) (*db.ListTopicsResponse, error) {

	fmt.Printf("[INFO]: Recieved list topics request\n")

	if !server.isTail {

		return nil, status.Error(codes.PermissionDenied, "Can't request to list topics on a server that isn't the tail server")
	}

	topicList := db.ListTopicsResponse{}
	topicList.Topics = []*db.Topic{}

	server.CRUDServer.topicLock.Lock()

	for key, val := range server.CRUDServer.topics {
		topicList.Topics = append(topicList.Topics, &db.Topic{Id: val, Name: key})
	}

	server.CRUDServer.topicLock.Unlock()

	return &topicList, nil
}

func (server *Server) GetMessages(ctx context.Context, req *db.GetMessagesRequest) (*db.GetMessagesResponse, error) {

	fmt.Printf("[INFO]: Recieved get messages request\n")

	if !server.isTail {

		return nil, status.Error(codes.PermissionDenied, "Can't request to get messages on a server that isn't the tail server")
	}

	topicPosts := db.GetMessagesResponse{}

	server.CRUDServer.postLock.Lock()

	topic, topicExists := server.CRUDServer.topicsPostsList[req.TopicId]

	server.CRUDServer.postLock.Unlock()

	postResponse := []*db.Message{}

	if !topicExists {

		return nil, status.Error(codes.NotFound, "Topic doesnt exist")
	}

	for i := req.FromMessageId; i < req.FromMessageId+int64(req.Limit); i++ {

		if i >= int64(len(topic)) {
			break
		}

		postResponse = append(postResponse, topic[i])
	}

	topicPosts.Messages = postResponse

	return &topicPosts, nil
}

func (server *Server) doTopicsExist(topicIds []int64) int64 {

	server.CRUDServer.topicLock.Lock()

	for _, topicId := range topicIds {

		foundTopic := false

		for _, availableTopicId := range server.CRUDServer.topics {

			if availableTopicId == topicId {
				foundTopic = true
				break
			}
		}

		if !foundTopic {

			server.CRUDServer.topicLock.Unlock()
			return topicId
		}
	}

	server.CRUDServer.topicLock.Unlock()

	return 0
}

func (server *Server) SubscribeTopic(req *db.SubscribeTopicRequest, stream db.MessageBoard_SubscribeTopicServer) error {

	fmt.Printf("[INFO]: recieved subscription request\n")

	server.CRUDServer.userSubscriptionLock.Lock()

	_, userPermited := server.CRUDServer.userSubscription[req.SubscribeToken]

	server.CRUDServer.userSubscriptionLock.Unlock()

	if server.userExists(req.UserId) == "" {

		fmt.Printf("[INFO]: User doesnt exist\n")
		return status.Error(codes.NotFound, "User doesnt exist")
	}

	if !userPermited {

		return status.Error(codes.PermissionDenied, "This user hasnt been authorized to subscribe to this topic")
	}

	if req.FromMessageId < 1 {

		return status.Error(codes.InvalidArgument, "Starting message id has to be a value above 0")
	}

	err := server.doTopicsExist(req.TopicId)

	if err != 0 {

		return status.Error(codes.NotFound, "Requested topic doesnt exist")
	}

	sendNewSubscriptions := []SubscriptionData{}

	server.CRUDServer.userSubscriptionLock.Lock()

	for _, newTopic := range req.TopicId {

		alreadySubed := false

		for _, subedTopics := range server.CRUDServer.userSubscription[req.SubscribeToken] {

			if subedTopics == nil {

				continue
			}

			if subedTopics.topic == newTopic {

				alreadySubed = true
				break
			}
		}

		if !alreadySubed && newTopic > 0 {

			fmt.Printf("[INFO]: generated new subscription\n")

			newSubscription := &SubscriptionData{topic: newTopic, fromMsgIndex: req.FromMessageId}
			server.CRUDServer.userSubscription[req.SubscribeToken] = append(server.CRUDServer.userSubscription[req.SubscribeToken], newSubscription)
			sendNewSubscriptions = append(sendNewSubscriptions, *newSubscription)
		}

	}

	for _, newSub := range sendNewSubscriptions {

		server.CRUDServer.postLock.Lock()

		topicList := server.CRUDServer.topicsPostsList[newSub.topic]

		server.CRUDServer.postLock.Unlock()

		startMsgIndex := sort.Search(len(topicList), func(i int) bool {

			return topicList[i].Id >= newSub.fromMsgIndex
		})

		if startMsgIndex >= len(topicList) {

			startMsgIndex = len(topicList) - 1
		}

		if len(topicList) == 0 {

			startMsgIndex = 0
		}

		newSub.fromMsgIndex = int64(startMsgIndex)
		newSub.lastMsgIndex = int64(startMsgIndex)

	}

	ctx := stream.Context()

	for {

		select {
		case <-ctx.Done():

			fmt.Printf("[INFO]: Stopped sending\n")

			server.CRUDServer.userSubscriptionLock.Lock()

			for _, newSubTopic := range sendNewSubscriptions {

				for subIndex, subedTopic := range server.CRUDServer.userSubscription[req.SubscribeToken] {

					if subedTopic == nil {

						continue
					}

					if newSubTopic.topic == subedTopic.topic {

						server.CRUDServer.userSubscription[req.SubscribeToken][subIndex] = nil
						break
					}
				}
			}

			server.CRUDServer.userSubscriptionLock.Unlock()

			return nil

		default:
			for subIndex, subData := range sendNewSubscriptions {

				server.CRUDServer.postLock.Lock()

				topic := server.CRUDServer.topicsPostsList[subData.topic]

				server.CRUDServer.postLock.Unlock()

				if subData.lastMsgIndex != int64(len(topic)-1) {

					for newData := subData.lastMsgIndex + 1; newData < int64(len(topic)); newData++ {

						newEvent := db.MessageEvent{SequenceNumber: newData, Message: topic[newData], EventAt: timestamppb.Now()}

						fmt.Printf("[INFO]: sending new post %s\n", newEvent.Message.Text)

						err := stream.Send(&newEvent)

						if err != nil {

							return err
						}

						sendNewSubscriptions[subIndex].lastMsgIndex = newData

					}

				}
			}

			time.Sleep(time.Second)
		}
	}

}

func (server *Server) GetUsers(ctx context.Context, req *emptypb.Empty) (*db.UserResponse, error) {

	fmt.Printf("[INFO]: Recieved get users request\n")

	if !server.isTail {

		return nil, status.Error(codes.PermissionDenied, "Can't request to list users on a server that isn't the tail server")
	}

	users := db.UserResponse{User: []*db.User{}}

	server.CRUDServer.userLock.Lock()

	for username, userId := range server.CRUDServer.users {

		users.User = append(users.User, &db.User{Name: username, Id: userId})
	}

	server.CRUDServer.userLock.Unlock()

	return &users, nil
}

func (server *Server) GenerateSubscriptionNodes(ip string, port int) {

	server.nodes = make([]*db.NodeInfo, server.numOfServer)

	for nodeId := 0; nodeId < server.numOfServer; nodeId++ {

		server.nodes[nodeId] = &db.NodeInfo{NodeId: fmt.Sprintf("%d", nodeId), Address: fmt.Sprintf("%v:%v", ip, port+nodeId)}
	}
}

func checkError(err error) {

	if err != nil {
		panic(fmt.Sprintf("[ERROR]: %s", err))
	}

}

func startServer(ip string, port int, nodeId string, isHead bool, isTail bool, numOfServers int) {

	currUrl := fmt.Sprintf("%v:%v", ip, port)

	CRUD := ServerDataBase{}
	CRUD.users = make(map[string]int64)
	CRUD.userIndex = 1

	CRUD.topics = make(map[string]int64)
	CRUD.topicIndex = 1

	CRUD.topicsPosts = make(map[int64]map[int64]*db.Message)
	CRUD.topicsPostsList = make(map[int64][]*db.Message)
	CRUD.postIndex = 1

	CRUD.userLikes = make(map[int64][]int64)

	CRUD.userSubscription = make(map[string][]*SubscriptionData)

	fmt.Printf("Server starting: %s\n", currUrl)
	grpcServer := grpc.NewServer()

	server := Server{}
	server.CRUDServer = &CRUD
	server.url = currUrl
	server.nodeId = nodeId
	server.isHead = isHead
	server.isTail = isTail
	server.numOfServer = numOfServers
	server.nextSub = 0

	db.RegisterSyncDataServer(grpcServer, &server)

	if !isTail {

		server.nextServerUrl = fmt.Sprintf("%v:%v", ip, port+1)

		go func() {
			fmt.Printf("[INFO]: Trying to connect to the next server in chain\n")

			for {

				nextConn, _ := grpc.NewClient(server.nextServerUrl, grpc.WithTransportCredentials(insecure.NewCredentials()))
				server.syncDataService = db.NewSyncDataClient(nextConn)

				_, err := server.syncDataService.SyncPing(context.Background(), &emptypb.Empty{})

				if err == nil {

					fmt.Printf("[INFO]: Successfully connected to the next server in chain\n")

					break
				} else {

					fmt.Printf("[INFO]: Failed to connect to the next server in chain retrying\n")
					time.Sleep(2 * time.Second)
				}
			}
		}()
	} else {

		server.nextServerUrl = ""
	}

	if server.isHead {

		server.GenerateSubscriptionNodes(ip, port)
	}

	db.RegisterMessageBoardServer(grpcServer, &server)

	listener, err := net.Listen("tcp", currUrl)
	checkError(err)

	err = grpcServer.Serve(listener)
	checkError(err)

}

func main() {

	iPtr := flag.String("ip", "localhost", "server IP")
	pPtr := flag.Int("p", 6000, "server port")
	nPtr := flag.String("n", "0", "Node id")
	hptr := flag.Bool("h", false, "Is the server a head server")
	tptr := flag.Bool("t", false, "Is the server a tail server")
	sptr := flag.Int("s", 4, "Number of servers")

	flag.Parse()

	ip := *iPtr
	port := *pPtr
	nodeId := *nPtr
	isHead := *hptr
	isTail := *tptr
	numOfServers := *sptr

	startServer(ip, port, nodeId, isHead, isTail, numOfServers)
}
