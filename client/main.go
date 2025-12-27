package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"time"

	db "github.com/zanoixo/VPSA-project/razpravljalnica"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

func checkError(err error) {

	if err != nil {
		panic(fmt.Sprintf("[ERROR]: %s", err))
	}

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
	fmt.Printf("[INFO]: sent a create user request\n")
	checkError(err)
	fmt.Printf("[INFO]: recived user ID request %d\n", createUsrResp.Id)

	return createUsrResp, nil

}

func (client *Client) CreateTopic(name string) (*db.Topic, error) {

	newTopicReq := &db.CreateTopicRequest{Name: name}

	createTopicResp, err := client.msgBoardClient.CreateTopic(context.Background(), newTopicReq)
	fmt.Printf("[INFO] sent a create topic request\n")
	checkError(err)

	return createTopicResp, nil
}

func (client *Client) PostMessage(topicID, userID int64, text string) (*db.Message, error) {

	newPostReq := &db.PostMessageRequest{TopicId: topicID, UserId: userID, Text: text}

	PostResp, err := client.msgBoardClient.PostMessage(context.Background(), newPostReq)
	fmt.Printf("[INFO] sent a post message request\n")
	checkError(err)

	return PostResp, nil
}

func (client *Client) LikeMessage(topicID, messageID, userID int64) (*db.Message, error) {

	newLikeReq := &db.LikeMessageRequest{TopicId: topicID, MessageId: messageID, UserId: userID}

	LikeResp, err := client.msgBoardClient.LikeMessage(context.Background(), newLikeReq)
	fmt.Printf("[INFO] sent a like message request\n")
	checkError(err)

	return LikeResp, nil
}

func (client *Client) GetSubscriptionNode(userID int64, topicIDs []int64) (*db.SubscriptionNodeResponse, error) {

	subNodeReq := &db.SubscriptionNodeRequest{UserId: userID, TopicId: topicIDs}

	SubNodeResp, err := client.msgBoardClient.GetSubscriptionNode(context.Background(), subNodeReq)
	fmt.Printf("[INFO] sent a get subscription node request\n")
	checkError(err)

	return SubNodeResp, nil
}

func (client *Client) ListTopics() (*db.ListTopicsResponse, error) {

	listTopicsReq := &emptypb.Empty{}

	listTopicsRes, err := client.msgBoardClient.ListTopics(context.Background(), listTopicsReq)
	fmt.Printf("[INFO] sent a list topics request\n")
	checkError(err)

	return listTopicsRes, nil
}

func (client *Client) GetMessages(topicID int64, fromMessageID int64, limit int32) (*db.GetMessagesResponse, error) {

	getMsgReq := &db.GetMessagesRequest{TopicId: topicID, FromMessageId: fromMessageID, Limit: limit}

	msgResp, err := client.msgBoardClient.GetMessages(context.Background(), getMsgReq)
	fmt.Printf("[INFO] sent a get messages request\n")
	checkError(err)

	return msgResp, nil
}

func (client *Client) recvTopicEvents(msgEvents chan *db.MessageEvent, req *db.SubscribeTopicRequest) error {
	defer close(msgEvents)

	msgStream, err := client.msgBoardClient.SubscribeTopic(context.Background(), req)
	fmt.Printf("[INFO] sent a subscribe topic request\n")
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
	fmt.Printf("[INFO] sent a get cluster state request\n")
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

	for {
		time.Sleep(time.Second)
	}

	return nil
}

func main() {

	iPtr := flag.String("ip", "localhost", "server IP")
	pPtr := flag.Int("p", 6000, "server port")
	nPtr := flag.String("n", "noName", "name of client")
	flag.Parse()

	url := fmt.Sprintf("%v:%v", *iPtr, *pPtr)

	startClient(url, *nPtr)

}
