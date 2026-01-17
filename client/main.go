package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"

	db "github.com/zanoixo/VPSA-project/razpravljalnica"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

func checkError(err error, client *Client) bool {

	if err != nil {
		fmt.Printf("[ERROR]: %s\n", err)

		if status.Code(err) == codes.PermissionDenied {

			fmt.Printf("[ERROR]: Retrying connection to the servers\n")
			client.controlClient.GetClusterState(context.Background(), &emptypb.Empty{})
		}
		return true
	}

	return false

}

type Client struct {
	headServer         string
	tailServer         string
	msgBoardHeadClient db.MessageBoardClient
	msgBoardSubClient  db.MessageBoardClient
	msgBoardTailClient db.MessageBoardClient
	controlClient      db.ControlPlaneClient
	name               string
	id                 int64
	otherUsers         map[int64]string
	availableTopics    map[string]int64
	idToTopic          map[int64]string
	subToken           string
	userLock           sync.Mutex
	topicsLock         sync.Mutex
}

func (client *Client) CreateUser(name string) (*db.User, error) {

	createUsrReq := &db.CreateUserRequest{Name: name}

	createUsrResp, err := client.msgBoardHeadClient.CreateUser(context.Background(), createUsrReq)

	if !checkError(err, client) {
		client.id = createUsrResp.Id
	}

	return createUsrResp, nil

}

func (client *Client) CreateTopic(name string) (*db.Topic, error) {

	newTopicReq := &db.CreateTopicRequest{Name: name}

	createTopicResp, err := client.msgBoardHeadClient.CreateTopic(context.Background(), newTopicReq)

	if !checkError(err, client) {
		fmt.Printf("Topic: %s created\n", createTopicResp.Name)
	} else {

		return nil, status.Error(codes.AlreadyExists, "Topic already exists")
	}

	client.topicsLock.Lock()

	client.availableTopics[name] = createTopicResp.Id
	client.idToTopic[createTopicResp.Id] = name

	client.topicsLock.Unlock()

	return createTopicResp, nil
}

func (client *Client) PostMessage(topicID, userID int64, text string) (*db.Message, error) {

	newPostReq := &db.PostMessageRequest{TopicId: topicID, UserId: userID, Text: text}

	PostResp, err := client.msgBoardHeadClient.PostMessage(context.Background(), newPostReq)

	if !checkError(err, client) {
		fmt.Printf("Post created: %s\n", PostResp.Text)
	}

	return PostResp, nil
}

func (client *Client) LikeMessage(topicID, messageID, userID int64) (*db.Message, error) {

	newLikeReq := &db.LikeMessageRequest{TopicId: topicID, MessageId: messageID, UserId: userID}

	LikeResp, err := client.msgBoardHeadClient.LikeMessage(context.Background(), newLikeReq)

	if !checkError(err, client) {

		fmt.Printf("Post successfuly liked\n")
	}

	return LikeResp, nil
}

func (client *Client) GetSubscriptionNode(userID int64, topicIDs []int64) (*db.SubscriptionNodeResponse, error) {

	subNodeReq := &db.SubscriptionNodeRequest{UserId: userID, TopicId: topicIDs}

	SubNodeResp, err := client.msgBoardHeadClient.GetSubscriptionNode(context.Background(), subNodeReq)

	if !checkError(err, client) {

		client.subToken = SubNodeResp.SubscribeToken
	}

	return SubNodeResp, nil
}

func (client *Client) updateTopicList() (*db.ListTopicsResponse, error) {

	listTopicsReq := &emptypb.Empty{}

	listTopicsRes, err := client.msgBoardTailClient.ListTopics(context.Background(), listTopicsReq)

	client.topicsLock.Lock()

	if !checkError(err, client) {

		for _, topic := range listTopicsRes.Topics {

			client.availableTopics[topic.Name] = topic.Id
			client.idToTopic[topic.Id] = topic.Name
		}

	} else {

		return nil, err
	}

	client.topicsLock.Unlock()

	return listTopicsRes, nil
}

func (client *Client) ListTopics() (*db.ListTopicsResponse, error) {

	listTopicsRes, err := client.updateTopicList()

	if err != nil {

		return nil, err
	}

	fmt.Printf("Available topics:\n")

	client.topicsLock.Lock()

	for _, topic := range listTopicsRes.Topics {

		fmt.Printf("Topic %s\n", topic.Name)
	}

	client.topicsLock.Unlock()

	return listTopicsRes, err
}

func (client *Client) GetMessages(topicID int64, fromMessageID int64, limit int32) (*db.GetMessagesResponse, error) {

	getMsgReq := &db.GetMessagesRequest{TopicId: topicID, FromMessageId: fromMessageID, Limit: limit}

	msgResp, err := client.msgBoardTailClient.GetMessages(context.Background(), getMsgReq)

	if !checkError(err, client) {

		for _, msg := range msgResp.Messages {

			if msg.Text != "" {

				username, userExists := client.otherUsers[msg.UserId]

				if !userExists {

					client.GetUsers()
				}

				fmt.Printf("[%s]@%s: %s - likes: %d\n", client.idToTopic[msg.TopicId], username, msg.Text, msg.Likes)

			}

		}
	}

	return msgResp, nil
}

func (client *Client) recvTopicEvents(msgEvents chan *db.MessageEvent, req *db.SubscribeTopicRequest) error {

	defer close(msgEvents)

	msgStream, err := client.msgBoardSubClient.SubscribeTopic(context.Background(), req)

	if checkError(err, client) {

		return err
	}

	for {

		newMsg, err := msgStream.Recv()

		checkError(err, client)

		if err == io.EOF {
			return nil
		}

		checkError(err, client)

		msgEvents <- newMsg

	}

}

func (client *Client) displayNewEvents(msgEvents chan *db.MessageEvent) {

	for newEvent := range msgEvents {

		fmt.Print("\r")
		fmt.Print("\033[K")

		_, exists := client.otherUsers[newEvent.Message.UserId]

		if !exists {

			client.GetUsers()
		}

		fmt.Printf("[%s]@%s: %s - likes: %d post id: %d\n", client.idToTopic[newEvent.Message.TopicId], client.otherUsers[newEvent.Message.UserId], newEvent.Message.Text, newEvent.Message.Likes, newEvent.Message.Id)
		fmt.Printf("$razpravljalnica@%s: ", client.name)
	}
}

func (client *Client) SubscribeTopic(topicIDs []int64, userID, fromMessageID int64, token string) (<-chan *db.MessageEvent, error) {

	subNode, _ := client.GetSubscriptionNode(client.id, topicIDs)

	fmt.Printf("Connecting to subscription server %s\n", subNode.Node.Address)
	subConn, err := grpc.NewClient(subNode.Node.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))

	if checkError(err, client) {

		return nil, status.Error(codes.Unavailable, "Subscription server not available")
	}

	client.msgBoardSubClient = db.NewMessageBoardClient(subConn)

	msgEvents := make(chan *db.MessageEvent)

	subTopicReq := &db.SubscribeTopicRequest{TopicId: topicIDs, UserId: userID, FromMessageId: fromMessageID, SubscribeToken: client.subToken}

	go client.recvTopicEvents(msgEvents, subTopicReq)

	go client.displayNewEvents(msgEvents)

	return msgEvents, nil
}

func (client *Client) GetClusterState() (*db.GetClusterStateResponse, error) {

	clusterStateReq := &emptypb.Empty{}

	clusterStateRes, err := client.controlClient.GetClusterState(context.Background(), clusterStateReq)
	checkError(err, client)

	client.headServer = clusterStateRes.Head.Address
	client.tailServer = clusterStateRes.Tail.Address

	fmt.Printf("Connecting to head server %s\n", client.headServer)
	headConn, err := grpc.NewClient(client.headServer, grpc.WithTransportCredentials(insecure.NewCredentials()))
	checkError(err, client)

	fmt.Printf("Connecting to tail server %s\n", client.tailServer)
	tailConn, err := grpc.NewClient(client.tailServer, grpc.WithTransportCredentials(insecure.NewCredentials()))
	checkError(err, client)

	client.msgBoardHeadClient = db.NewMessageBoardClient(headConn)
	client.msgBoardTailClient = db.NewMessageBoardClient(tailConn)

	return clusterStateRes, nil
}

func (client *Client) GetUsers() (*db.UserResponse, error) {

	getUsersReq := &emptypb.Empty{}

	getUsersRes, err := client.msgBoardTailClient.GetUsers(context.Background(), getUsersReq)

	client.userLock.Lock()

	if !checkError(err, client) {

		for _, user := range getUsersRes.User {

			client.otherUsers[user.Id] = user.Name
		}
	}

	client.userLock.Unlock()

	return getUsersRes, nil
}

func startClient(controlUrl string, name string) error {

	client := Client{}

	fmt.Printf("Connecting to control server %s\n", controlUrl)
	controlConn, err := grpc.NewClient(controlUrl, grpc.WithTransportCredentials(insecure.NewCredentials()))
	checkError(err, &client)

	client.controlClient = db.NewControlPlaneClient(controlConn)
	client.msgBoardSubClient = nil
	client.name = name
	client.otherUsers = make(map[int64]string)
	client.availableTopics = make(map[string]int64)
	client.idToTopic = map[int64]string{}

	client.GetClusterState()

	client.CreateUser(name)

	client.GetUsers()

	fmt.Printf("Welcome to razpravljalnica\n")

	fmt.Printf("\n")

	client.ListTopics()

	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Printf("$razpravljalnica@%s: ", client.name)

		input, _ := reader.ReadString('\n')
		input = input[:len(input)-1]
		args := strings.Split(input, " ")

		switch args[0] {
		case "newTopic":

			if len(args) != 2 {

				fmt.Printf("Wrong number of arguments use help to see the list of commands\n")
			} else {

				topicName := args[1]
				client.CreateTopic(topicName)
			}
		case "post":

			if len(args) < 3 {

				fmt.Printf("Wrong number of arguments use help to see the list of commands\n")
			} else {

				topic := args[1]

				postText := ""
				for i := 2; i < len(args); i++ {
					postText += " "
					postText += args[i]
				}

				client.PostMessage(client.availableTopics[topic], client.id, postText)

			}

		case "listTopics":

			client.ListTopics()
		case "like":

			if len(args) != 3 {

				fmt.Printf("Wrong number of arguments use help to see the list of commands\n")
			} else {

				topic := args[1]
				msgId, err := strconv.Atoi(args[2])

				if err != nil {

					fmt.Printf("Invalid id for topic or message\n")
				} else {

					client.LikeMessage(client.availableTopics[topic], int64(msgId), client.id)
				}
			}
		case "listPosts":

			if len(args) != 4 {

				fmt.Printf("Wrong number of arguments use help to see the list of commands\n")
			} else {

				topic := args[1]
				startingId, err1 := strconv.Atoi(args[2])
				numOfMsgs, err2 := strconv.Atoi(args[3])

				if err1 != nil || err2 != nil {

					fmt.Printf("Invalid starting id or number of msgs\n")
				} else {

					client.GetMessages(client.availableTopics[topic], int64(startingId), int32(numOfMsgs))
				}
			}
		case "getUsers":
			client.GetUsers()

		case "sub":

			client.updateTopicList()

			if len(args) < 3 {

				fmt.Printf("Wrong number of arguments use help to see the list of commands\n")
			} else {

				fromMsgId, err := strconv.Atoi(args[1])

				if err != nil {

					fmt.Printf("Start id must be a number\n")
				} else {

					newSubs := make([]int64, 1)

					for i := 2; i < len(args); i++ {

						subToTopic, topicExists := client.availableTopics[args[i]]

						if !topicExists {

							fmt.Printf("Topic doesnt exist %s\n", args[i])
							continue
						}

						newSubs = append(newSubs, subToTopic)

					}
					client.SubscribeTopic(newSubs, client.id, int64(fromMsgId), client.subToken)
				}

			}
		case "exit":
			return nil

		default:
			fmt.Printf("Help:\n")
			fmt.Printf("	help                                                 - displays all the commands\n")
			fmt.Printf("	exit                                                 - stops the program\n")
			fmt.Printf("	newTopic <name>                                      - creates a new topic with the name <name>\n")
			fmt.Printf("	listTopics                                           - displays and updates a list of all available topics and their Ids\n")
			fmt.Printf("	post <topicName> <msg>                               - creates a new post on the topic <topicName> with the text <msg> if the topic exists\n")
			fmt.Printf("	like <topicName> <msgId>                             - likes an existing post <msgId> within a topic <topicName> \n")
			fmt.Printf("	listPosts <topicName> <startingMsgId> <numberOfMsgs> - lists all posts from a topic <topicName> starting with a msgId <startingMsgId> up to the number of wanted posts <numberOfMsgs>\n")
			fmt.Printf("	getUsers                                             - refreshes the usernames of other users\n")
			fmt.Printf("	sub <startingMsgId> <topic names>                    - subscribes user to topics <topic names> and starts displaying all messages from post starting with id <startingMsgId> onward\n")

		}
	}
}

func main() {

	iPtr := flag.String("ip", "localhost", "server IP")
	pPtr := flag.Int("hp", 6000, "control server port")
	nPtr := flag.String("n", "noName", "name of client")
	flag.Parse()

	controlUrl := fmt.Sprintf("%v:%v", *iPtr, *pPtr)

	startClient(controlUrl, *nPtr)

}
