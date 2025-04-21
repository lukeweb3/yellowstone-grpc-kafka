package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
	gproto "google.golang.org/protobuf/proto"

	"consumer/proto"
)

func main() {
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.Consumer.Offsets.Initial = sarama.OffsetNewest

	consumerGroup, err := sarama.NewConsumerGroup(
		[]string{"localhost:9092"},
		"my-consumer-group",
		config,
	)
	if err != nil {
		log.Fatalf("Error creating consumer group: %v", err)
	}
	defer consumerGroup.Close()

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	handler := &ConsumerHandler{}

	go func() {
		for {
			if err := consumerGroup.Consume(
				context.Background(),
				[]string{"test-topic"},
				handler,
			); err != nil {
				log.Printf("Error from consumer: %v", err)
			}

			if context.Canceled != nil {
				return
			}
		}
	}()

	log.Println("Kafka consumer is running...")
	<-sigchan
	log.Println("Shutting down consumer")
}

type ConsumerHandler struct{}

func (h *ConsumerHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *ConsumerHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *ConsumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		// log.Printf("Received message: Topic(%s) Partition(%d) Offset(%d) Key(%s) Value(%s)\n",
		// message.Topic, message.Partition, message.Offset, string(message.Key), string(message.Value))
		tx := &proto.SubscribeUpdateTransactionInfo{}
		err := gproto.Unmarshal(message.Value, tx)
		if err != nil {
			fmt.Println("err: ", err)
		} else {
			fmt.Println("tx: ", tx)
		}

		session.MarkMessage(message, "")
	}
	return nil
}
