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

	db "github.com/zanoixo/VPSA-project/razpravljalnica"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

func checkError(err error) bool {

	if err != nil {
		fmt.Printf("[ERROR]: %s\n", err)
		return true
	}

	return false

}

type Client struct {
	conn           *grpc.ClientConn
	msgBoardClient db.MessageBoardClient
	controlClient  db.ControlPlaneClient
	name           string
	id             int64
}

func (client *Client) CreateUser(name string) (*db.User, error) {

	createUsrReq := &db.CreateUserRequest{Name: name}

	createUsrResp, err := client.msgBoardClient.CreateUser(context.Background(), createUsrReq)

	if !checkError(err) {
		client.id = createUsrResp.Id
	}

	return createUsrResp, nil

}

func (client *Client) CreateTopic(name string) (*db.Topic, error) {

	newTopicReq := &db.CreateTopicRequest{Name: name}

	createTopicResp, err := client.msgBoardClient.CreateTopic(context.Background(), newTopicReq)

	if !checkError(err) {
		fmt.Printf("Topic: %s created\n", createTopicResp.Name)
	}

	return createTopicResp, nil
}

func (client *Client) PostMessage(topicID, userID int64, text string) (*db.Message, error) {

	newPostReq := &db.PostMessageRequest{TopicId: topicID, UserId: userID, Text: text}

	PostResp, err := client.msgBoardClient.PostMessage(context.Background(), newPostReq)

	if !checkError(err) {
		fmt.Printf("Post created: %s\n", PostResp.Text)
	}

	return PostResp, nil
}

func (client *Client) LikeMessage(topicID, messageID, userID int64) (*db.Message, error) {

	newLikeReq := &db.LikeMessageRequest{TopicId: topicID, MessageId: messageID, UserId: userID}

	LikeResp, err := client.msgBoardClient.LikeMessage(context.Background(), newLikeReq)

	if !checkError(err) {

		fmt.Printf("Post successfuly liked\n")
	}

	return LikeResp, nil
}

func (client *Client) GetSubscriptionNode(userID int64, topicIDs []int64) (*db.SubscriptionNodeResponse, error) {

	subNodeReq := &db.SubscriptionNodeRequest{UserId: userID, TopicId: topicIDs}

	SubNodeResp, err := client.msgBoardClient.GetSubscriptionNode(context.Background(), subNodeReq)
	checkError(err)

	return SubNodeResp, nil
}

func (client *Client) ListTopics() (*db.ListTopicsResponse, error) {

	listTopicsReq := &emptypb.Empty{}

	listTopicsRes, err := client.msgBoardClient.ListTopics(context.Background(), listTopicsReq)

	if !checkError(err) {

		for i := 0; i < len(listTopicsRes.Topics); i++ {
			fmt.Printf("Topic %s id: %d\n", listTopicsRes.Topics[i].Name, listTopicsRes.Topics[i].Id)
		}

	}

	return listTopicsRes, nil
}

func (client *Client) GetMessages(topicID int64, fromMessageID int64, limit int32) (*db.GetMessagesResponse, error) {

	getMsgReq := &db.GetMessagesRequest{TopicId: topicID, FromMessageId: fromMessageID, Limit: limit}

	msgResp, err := client.msgBoardClient.GetMessages(context.Background(), getMsgReq)

	if !checkError(err) {

		for _, msg := range msgResp.Messages {

			if msg.Text != "" {

				fmt.Printf("Post id: %d, likes: %d, msg: %s\n", msg.Id, msg.Likes, msg.Text)
			}

		}
	}

	return msgResp, nil
}

func (client *Client) recvTopicEvents(msgEvents chan *db.MessageEvent, req *db.SubscribeTopicRequest) error {
	defer close(msgEvents)

	msgStream, err := client.msgBoardClient.SubscribeTopic(context.Background(), req)
	checkError(err)

	for {

		newMsg, err := msgStream.Recv()

		if err == io.EOF {
			return nil
		}
		checkError(err)

		msgEvents <- newMsg
	}

}

func (client *Client) SubscribeTopic(topicIDs []int64, userID, fromMessageID int64, token string) (<-chan *db.MessageEvent, error) {

	msgEvents := make(chan *db.MessageEvent)

	subTopicReq := &db.SubscribeTopicRequest{TopicId: topicIDs, UserId: userID, FromMessageId: fromMessageID, SubscribeToken: token}

	go client.recvTopicEvents(msgEvents, subTopicReq)

	return msgEvents, nil
}

func (client *Client) GetClusterState() (*db.GetClusterStateResponse, error) {

	clusterStateReq := &emptypb.Empty{}

	clusterStateRes, err := client.controlClient.GetClusterState(context.Background(), clusterStateReq)
	checkError(err)

	return clusterStateRes, nil
}

func startClient(url string, name string) error {

	client := Client{}

	fmt.Printf("Connecting to server %s\n", url)
	conn, err := grpc.NewClient(url, grpc.WithTransportCredentials(insecure.NewCredentials()))
	checkError(err)

	client.conn = conn
	client.msgBoardClient = db.NewMessageBoardClient(conn)
	client.name = name
	client.CreateUser(name)

	fmt.Printf("Welcome to razpravljalnica\n")

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

				client.CreateTopic(args[1])
			}
		case "post":
			if len(args) < 3 {

				fmt.Printf("Wrong number of arguments use help to see the list of commands\n")
			} else {

				val, err := strconv.Atoi(args[1])
				if err != nil {

					fmt.Printf("Invalid id for topic\n")
				} else {

					postText := ""
					for i := 2; i < len(args); i++ {
						postText += " "
						postText += args[i]
					}

					client.PostMessage(int64(val), client.id, postText)

				}

			}

		case "listTopics":

			client.ListTopics()
		case "like":

			if len(args) != 3 {

				fmt.Printf("Wrong number of arguments use help to see the list of commands\n")
			} else {

				topicId, err1 := strconv.Atoi(args[1])
				msgId, err2 := strconv.Atoi(args[2])

				if err1 != nil || err2 != nil {

					fmt.Printf("Invalid id for topic or message\n")
				} else {

					client.LikeMessage(int64(topicId), int64(msgId), client.id)
				}
			}
		case "listPosts":

			if len(args) != 4 {

				fmt.Printf("Wrong number of arguments use help to see the list of commands\n")
			} else {

				topicId, err1 := strconv.Atoi(args[1])
				startingId, err2 := strconv.Atoi(args[2])
				numOfMsgs, err3 := strconv.Atoi(args[3])

				if err1 != nil || err2 != nil || err3 != nil {

					fmt.Printf("Invalid starting id or topic id or wrong number of msgs\n")
				} else {

					client.GetMessages(int64(topicId), int64(startingId), int32(numOfMsgs))
				}
			}
		case "exit":
			return nil
		default:
			fmt.Printf("Help:\n")
			fmt.Printf("	help                                               - displays all the commands\n")
			fmt.Printf("	exit                                               - stops the program\n")
			fmt.Printf("	newTopic <name>                                    - creates a new topic with the name <name>\n")
			fmt.Printf("	listTopics                                         - displays a list of all available topics and their Ids\n")
			fmt.Printf("	post <topicId> <msg>                               - creates a new post on the topic <topicId> with the text <msg> if the topic exists\n")
			fmt.Printf("	like <topicId> <msgId>                             - likes an existing post <msgId> within a topic <topicId> \n")
			fmt.Printf("	listPosts <topicId> <startingMsgId> <numberOfMsgs> - lists all posts from a topic <topicId> starting with a msgId <startingMsgId> up to the number of wanted posts <numberOfMsgs>\n")

		}
	}
}

func main() {

	iPtr := flag.String("ip", "localhost", "server IP")
	pPtr := flag.Int("p", 6000, "server port")
	nPtr := flag.String("n", "noName", "name of client")
	flag.Parse()

	url := fmt.Sprintf("%v:%v", *iPtr, *pPtr)

	startClient(url, *nPtr)

}
