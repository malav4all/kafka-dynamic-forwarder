package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/malav4all/kafka-dynamic-forwarder/config"

	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/segmentio/kafka-go"
)

// getTopics fetches the list of all available Kafka topics using confluent-kafka-go.
func getTopics() ([]string, error) {
	adminClient, err := ckafka.NewAdminClient(&ckafka.ConfigMap{"bootstrap.servers": config.BrokerAddress})
	if err != nil {
		return nil, fmt.Errorf("failed to create admin client: %w", err)
	}
	defer adminClient.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	deadline, ok := ctx.Deadline()
	if !ok {
		return nil, fmt.Errorf("no deadline set in context")
	}

	metadata, err := adminClient.GetMetadata(nil, false, int(deadline.Unix()))
	if err != nil {
		return nil, fmt.Errorf("failed to get metadata: %w", err)
	}

	var topicNames []string
	for topic := range metadata.Topics {
		topicNames = append(topicNames, topic)
	}
	return topicNames, nil
}

// createConsumer initializes a Kafka consumer for a specified topic and partition.
func createConsumer(topic string, partition int) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{config.BrokerAddress},
		Topic:     topic,
		Partition: partition,
		MinBytes:  1e3,  // 1KB
		MaxBytes:  10e6, // 10MB
	})
}

// createProducer initializes a Kafka producer for a specified topic.
func createProducer(topic string) *kafka.Writer {
	return kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{config.BrokerAddress},
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	})
}

// determineOutputTopic determines the output topic based on message data.
// This function can be customized as needed.
func determineOutputTopic(message kafka.Message) string {
	var msgContent map[string]interface{}
	if err := json.Unmarshal(message.Value, &msgContent); err == nil {
		if topic, ok := msgContent["destination_topic"].(string); ok {
			return topic
		}
	}
	return "default-topic" // Use a default topic if no specific topic is specified
}

func main() {
	topics, err := getTopics()
	if err != nil {
		log.Fatalf("Failed to fetch topics: %v", err)
	}

	// Start a consumer for each topic
	for _, topic := range topics {
		go func(topic string) {
			consumer := createConsumer(topic, 0) // Using partition 0 for simplicity
			defer consumer.Close()

			fmt.Printf("Listening to topic: %s\n", topic)

			for {
				msg, err := consumer.ReadMessage(context.Background())
				if err != nil {
					log.Printf("Error reading message from topic %s: %v\n", topic, err)
					break
				}

				fmt.Printf("Received message from topic %s: %s\n", msg.Topic, string(msg.Value))

				outputTopic := determineOutputTopic(msg)
				producer := createProducer(outputTopic)
				defer producer.Close()

				err = producer.WriteMessages(context.Background(), kafka.Message{
					Key:   msg.Key,
					Value: msg.Value,
				})
				if err != nil {
					log.Printf("Error forwarding message to %s: %v\n", outputTopic, err)
				} else {
					fmt.Printf("Message forwarded to topic %s\n", outputTopic)
				}
			}
		}(topic)
	}

	select {} // Keep the main goroutine running
}
