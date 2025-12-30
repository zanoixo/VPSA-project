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
	CRUDServer *ServerDataBase
	url        string
	nodeId     string
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

		server.CRUDServer.userLikes[currUser.Id] = []int64{}

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
		return nil, status.Error(codes.AlreadyExists, "Topic already exist")
	} else {

		fmt.Printf("[INFO]: Creating topic with ID %d\n", server.CRUDServer.topicIndex)

		server.CRUDServer.topicLock.Lock()

		server.CRUDServer.topics[req.Name] = server.CRUDServer.topicIndex
		server.CRUDServer.topicsPosts[server.CRUDServer.topicIndex] = make(map[int64]*db.Message)
		server.CRUDServer.topicsPostsList[server.CRUDServer.topicIndex] = []*db.Message{}
		newTopic.Id = server.CRUDServer.topicIndex
		server.CRUDServer.topicIndex++

		server.CRUDServer.topicLock.Unlock()

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

	_, topicExists := server.CRUDServer.topicsPosts[req.TopicId]

	if !topicExists {

		return nil, status.Error(codes.NotFound, "Topic doesnt exist")
	}

	server.CRUDServer.postLock.Lock()

	newPost.Id = server.CRUDServer.postIndex
	server.CRUDServer.topicsPosts[req.TopicId][server.CRUDServer.postIndex] = newPost
	server.CRUDServer.topicsPostsList[req.TopicId] = append(server.CRUDServer.topicsPostsList[req.TopicId], newPost)

	server.CRUDServer.postIndex++

	server.CRUDServer.postLock.Unlock()

	return newPost, nil
}

func (server *Server) userExists(userId int64) string {

	user := ""

	for username, id := range server.CRUDServer.users {

		if id == userId {

			return username
		}
	}

	return user

}

func (server *Server) alreadyLiked(userId int64, messageId int64) bool {

	return slices.Contains(server.CRUDServer.userLikes[userId], messageId)
}

func (server *Server) LikeMessage(ctx context.Context, req *db.LikeMessageRequest) (*db.Message, error) {

	fmt.Printf("[INFO]: Like message request recieved\n")

	likedMsg, msgExists := server.CRUDServer.topicsPosts[req.TopicId][req.MessageId]

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

	server.CRUDServer.likesLock.Lock()

	server.CRUDServer.topicsPosts[req.TopicId][req.MessageId].Likes++
	server.CRUDServer.userLikes[req.UserId] = append(server.CRUDServer.userLikes[req.UserId], req.MessageId)

	server.CRUDServer.likesLock.Unlock()

	fmt.Printf("[INFO]: Post liked\n")

	return likedMsg, nil
}

func (server *Server) GetSubscriptionNode(ctx context.Context, req *db.SubscriptionNodeRequest) (*db.SubscriptionNodeResponse, error) {

	fmt.Printf("[INFO]: recieved getSubscription request\n")

	subNode := &db.NodeInfo{}
	subNode.NodeId = server.nodeId
	subNode.Address = server.url

	user := server.userExists(req.UserId)

	if user == "" {

		fmt.Printf("[INFO]: User doesnt exist\n")
		return nil, status.Error(codes.NotFound, "User doesnt exist")
	}

	userToken := sha256.Sum256([]byte(user))

	subToken := hex.EncodeToString(userToken[:])

	_, userTokenExists := server.CRUDServer.userSubscription[subToken]

	if !userTokenExists {

		server.CRUDServer.userSubscriptionLock.Lock()

		server.CRUDServer.userSubscription[subToken] = []*SubscriptionData{}

		server.CRUDServer.userSubscriptionLock.Unlock()

		fmt.Printf("[INFO]: generated userToken\n")
	}

	subResp := &db.SubscriptionNodeResponse{Node: subNode, SubscribeToken: subToken}

	return subResp, nil
}

func (server *Server) ListTopics(ctx context.Context, req *emptypb.Empty) (*db.ListTopicsResponse, error) {

	fmt.Printf("[INFO]: Recieved list topics request\n")

	topicList := db.ListTopicsResponse{}
	topicList.Topics = []*db.Topic{}

	for key, val := range server.CRUDServer.topics {
		topicList.Topics = append(topicList.Topics, &db.Topic{Id: val, Name: key})
	}

	return &topicList, nil
}

func (server *Server) GetMessages(ctx context.Context, req *db.GetMessagesRequest) (*db.GetMessagesResponse, error) {

	topicPosts := db.GetMessagesResponse{}

	topic, topicExists := server.CRUDServer.topicsPostsList[req.TopicId]

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

	for _, topicId := range topicIds {

		foundTopic := false

		for _, availableTopicId := range server.CRUDServer.topics {

			if availableTopicId == topicId {
				foundTopic = true
				break
			}
		}

		if !foundTopic {

			return topicId
		}
	}

	return 0
}

func (server *Server) SubscribeTopic(req *db.SubscribeTopicRequest, stream db.MessageBoard_SubscribeTopicServer) error {

	fmt.Printf("[INFO]: recieved subscription request\n")

	_, userPermited := server.CRUDServer.userSubscription[req.SubscribeToken]

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

	server.CRUDServer.userSubscriptionLock.Unlock()

	for _, newSub := range sendNewSubscriptions {

		topicList := server.CRUDServer.topicsPostsList[newSub.topic]

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

	for {

		for subIndex, subData := range sendNewSubscriptions {

			topic := server.CRUDServer.topicsPostsList[subData.topic]

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

func (server *Server) GetUsers(ctx context.Context, req *emptypb.Empty) (*db.UserResponse, error) {

	users := db.UserResponse{User: []*db.User{}}

	for username, userId := range server.CRUDServer.users {

		users.User = append(users.User, &db.User{Name: username, Id: userId})
	}

	return &users, nil
}

func checkError(err error) {

	if err != nil {
		panic(fmt.Sprintf("[ERROR]: %s", err))
	}

}

func startServer(url string, nodeId string) {

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

	fmt.Printf("Server starting: %s\n", url)
	grpcServer := grpc.NewServer()

	server := Server{}
	server.CRUDServer = &CRUD
	server.url = url
	server.nodeId = nodeId

	db.RegisterMessageBoardServer(grpcServer, &server)

	listener, err := net.Listen("tcp", url)
	checkError(err)

	err = grpcServer.Serve(listener)
	checkError(err)

}

func main() {

	iPtr := flag.String("ip", "localhost", "server IP")
	pPtr := flag.Int("p", 6000, "server port")
	nPtr := flag.String("n", "0", "Node id")
	flag.Parse()

	url := fmt.Sprintf("%v:%v", *iPtr, *pPtr)

	startServer(url, *nPtr)
}
