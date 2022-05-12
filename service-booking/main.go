package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	readTopic         = "TestTopic1"
	writeTopic         = "TestTopic2"
	brokerAddress = "localhost:9092"
)

var (
	kafkaWriter *kafka.Writer
	kafkaReader *kafka.Reader

	kafkaWriteLogger  *log.Logger
	kafkaReaderLogger *log.Logger
)

func InitProducer() {
	kafkaWriteLogger = log.New(os.Stdout, "[BOOKING] Kafka writer: ", 0)

	// Intialize the writer with the broker addresses, and the topic
	kafkaWriter = kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{brokerAddress},
		Topic:   writeTopic,
		Logger:  kafkaWriteLogger, // Assign the logger to the writer
	})
}

func InitConsumer() {
	kafkaReaderLogger = log.New(os.Stdout, "[BOOKING] Kafka reader: ", 0)

	// Initialize a new reader with the brokers and topic
	// The groupID identifies the consumer and prevents it from receiving duplicate messages
	kafkaReader = kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokerAddress},
		Topic:   readTopic,
		GroupID: "booking-service",
		Logger:  kafkaReaderLogger, // Assign the logger to the reader
	})
}

func Produce(ctx context.Context, key string, message string) {
	// Each kafka message has a key and value.
	// The key is used to decide which partition (and consequently, which broker) the message gets published on
	err := kafkaWriter.WriteMessages(ctx, kafka.Message{
		Key:   []byte(key),
		Value: []byte("Key = " + key + ". Message = " + message), // Create an arbitrary message payload for the value
	})

	if err != nil {
		panic("Could not write message " + err.Error())
	}
}

func Consume(ctx context.Context) {
	for {
		// The `ReadMessage` method blocks until we receive the next event
		msg, err := kafkaReader.ReadMessage(ctx)

		if err != nil {
			panic("Could not read message " + err.Error())
		}

		// After receiving the message, log its value
		fmt.Println("[BOOKING] Received: ", string(msg.Value))
	}
}

func main() {
	ctx := context.Background()
	InitProducer()
	InitConsumer()

	go SendMessageToQueueExample(ctx)

	Consume(ctx)
}

func SendMessageToQueueExample(ctx context.Context) {
	i := 1

	for {
		Produce(ctx, strconv.Itoa(i), "This is message "+strconv.Itoa(i))
		i++
		time.Sleep(5 * time.Second)
	}
}
