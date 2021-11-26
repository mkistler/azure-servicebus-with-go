package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus/admin"
)

var (
	connStr = os.Getenv("CONNECTION_STRING")
	client  *azservicebus.Client
)

func init() {
	var err error
	client, err = azservicebus.NewClientFromConnectionString(connStr, nil)
	if err != nil {
		log.Fatalf("%v", err)
	}
}

func usage() {
	fmt.Println("Usage:")
	fmt.Println("\t foo send|receive queueName")
	os.Exit(1)
}

func main() {

	args := os.Args[1:] // exclude program
	if len(args) < 1 {
		usage()
	}

	switch args[0] {
	case "send":
		send(args[1:])
	case "receive":
		receive(args[1:])
	case "list-queues":
		list()
	}
}

func send(argSlice []string) {
	if len(argSlice) < 1 {
		usage()
	}
	queueName := argSlice[0]

	qSender, err := client.NewSender(queueName, nil)
	if err != nil {
		log.Fatalf("%v", err)
	}

	byteBuf, err := ioutil.ReadAll(os.Stdin)
	if err != nil {
		log.Fatalf("%v", err)
	}

	message := azservicebus.Message{Body: byteBuf}
	qSender.SendMessage(context.TODO(), &message)
}

func receive(argSlice []string) {
	if len(argSlice) < 1 {
		usage()
	}
	queueName := argSlice[0]

	qReceiver, err := client.NewReceiverForQueue(queueName, nil)
	if err != nil {
		log.Fatalf("%v", err)
	}

	messages, err := qReceiver.ReceiveMessages(context.TODO(), 1, nil)
	if err != nil {
		log.Fatalf("%v", err)
	}

	if len(messages) > 0 {
		message := messages[0]
		body, err := message.Body()
		if err != nil {
			log.Fatalf("%v", err)
		}
		fmt.Print(string(body))
		err = qReceiver.CompleteMessage(context.TODO(), message)
		if err != nil {
			log.Fatalf("%v", err)
		}
	}
}

func list() {
	adminClient, err := admin.NewClientFromConnectionString(connStr, nil)
	if err != nil {
		log.Fatalf("%v", err)
	}
	queuePager := adminClient.ListQueues(nil)

	for queuePager.NextPage(context.TODO()) {
		for _, queue := range queuePager.PageResponse().Items {
			fmt.Printf("Queue name: %s, max size in MB: %d\n", queue.QueueName, *queue.MaxSizeInMegabytes)
		}
	}

	if queuePager.Err() != nil {
		log.Fatalf("%v", queuePager.Err())
	}
}
