package main

import (
	"context"
	"flag"
	"fmt"
	"io"

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

func (client *Client) CreateUser(name string) *db.User {

	createUsrReq := &db.CreateUserRequest{Name: name}

	createUsrResp, err := client.msgBoardClient.CreateUser(context.Background(), createUsrReq)
	checkError(err)

	return createUsrResp

}

func (client *Client) CreateTopic(name string) *db.Topic {

	newTopicReq := &db.CreateTopicRequest{Name: name}

	createTopicResp, err := client.msgBoardClient.CreateTopic(context.Background(), newTopicReq)
	checkError(err)

	return createTopicResp
}

func (client *Client) PostMessage(topicID, userID int64, text string) *db.Message {

	newPostReq := &db.PostMessageRequest{TopicId: topicID, UserId: userID, Text: text}

	PostResp, err := client.msgBoardClient.PostMessage(context.Background(), newPostReq)
	checkError(err)

	return PostResp
}

func (client *Client) LikeMessage(topicID, messageID, userID int64) *db.Message {

	newLikeReq := &db.LikeMessageRequest{TopicId: topicID, MessageId: messageID, UserId: userID}

	LikeResp, err := client.msgBoardClient.LikeMessage(context.Background(), newLikeReq)
	checkError(err)

	return LikeResp
}

func (client *Client) GetSubscriptionNode(userID int64, topicIDs []int64) *db.SubscriptionNodeResponse {

	subNodeReq := &db.SubscriptionNodeRequest{UserId: userID, TopicId: topicIDs}

	SubNodeResp, err := client.msgBoardClient.GetSubscriptionNode(context.Background(), subNodeReq)
	checkError(err)

	return SubNodeResp
}

func (client *Client) ListTopics() *db.ListTopicsResponse {

	listTopicsReq := &emptypb.Empty{}

	listTopicsRes, err := client.msgBoardClient.ListTopics(context.Background(), listTopicsReq)
	checkError(err)

	return listTopicsRes
}

func (client *Client) GetMessages(topicID int64, fromMessageID int64, limit int32) *db.GetMessagesResponse {

	getMsgReq := &db.GetMessagesRequest{TopicId: topicID, FromMessageId: fromMessageID, Limit: limit}

	msgResp, err := client.msgBoardClient.GetMessages(context.Background(), getMsgReq)
	checkError(err)

	return msgResp
}

func (client *Client) recvTopicEvents(msgEvents chan *db.MessageEvent, req *db.SubscribeTopicRequest) {
	defer close(msgEvents)

	msgStream, err := client.msgBoardClient.SubscribeTopic(context.Background(), req)
	checkError(err)

	for {

		newMsg, err := msgStream.Recv()

		if err == io.EOF {
			return
		}
		checkError(err)

		msgEvents <- newMsg
	}

}

func (client *Client) SubscribeTopic(topicIDs []int64, userID, fromMessageID int64, token string) <-chan *db.MessageEvent {

	msgEvents := make(chan *db.MessageEvent)

	subTopicReq := &db.SubscribeTopicRequest{TopicId: topicIDs, UserId: userID, FromMessageId: fromMessageID, SubscribeToken: token}

	go client.recvTopicEvents(msgEvents, subTopicReq)

	return msgEvents
}

func (client *Client) GetClusterState() *db.GetClusterStateResponse {

	clusterStateReq := &emptypb.Empty{}

	clusterStateRes, err := client.controlClient.GetClusterState(context.Background(), clusterStateReq)
	checkError(err)

	return clusterStateRes
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
