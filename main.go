package main

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/eiladin/policy-bushost/config"
)

func main() {
	config := config.InitConfig()

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(config.Sqs.Region),
	})

	if err != nil {
		fmt.Println("Error", err)
		return
	}

	svc := sqs.New(sess)

	queue, err := svc.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String(config.Sqs.QueueName),
	})
	if err != nil {
		fmt.Println("Error", err)
		return
	}
	fmt.Println("Listening on: ", *queue.QueueUrl)

	chnMessages := make(chan *sqs.Message, 10)
	go pollSqs(chnMessages, queue.QueueUrl, svc)

	for message := range chnMessages {
		handleMessage(message)
		deleteMessage(message, queue.QueueUrl, svc)
	}
}

func pollSqs(chn chan<- *sqs.Message, queueUrl *string, svc *sqs.SQS) {
	for {
		fmt.Println("worker: Start polling")
		output, err := svc.ReceiveMessage(&sqs.ReceiveMessageInput{
			QueueUrl:              queueUrl,
			MessageAttributeNames: aws.StringSlice([]string{"All"}),
			MaxNumberOfMessages:   aws.Int64(10),
			WaitTimeSeconds:       aws.Int64(10),
		})

		if err != nil {
			fmt.Printf("failed to fetch sqs message %v", err)
		}

		for _, message := range output.Messages {
			chn <- message
		}
	}
}

func handleMessage(message *sqs.Message) {
	p := message.MessageAttributes["Product"]
	a := message.MessageAttributes["App"]
	v := message.MessageAttributes["Version"]

	fmt.Printf("%s-%s-%s: %s\n", *p.StringValue, *a.StringValue, *v.StringValue, *message.Body)
}

func deleteMessage(message *sqs.Message, queueUrl *string, svc *sqs.SQS) {
	_, err := svc.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      queueUrl,
		ReceiptHandle: message.ReceiptHandle,
	})

	if err != nil {
		fmt.Println("Delete Error", err)
	}
	fmt.Println("Message Deleted: ", *message.MessageId)
}
