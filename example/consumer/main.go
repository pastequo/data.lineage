package main

import (
	"context"
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
)

func main() {
	ctx := context.Background()

	config := sarama.NewConfig()
	config.Version = sarama.V3_3_1_0

	kc, err := sarama.NewConsumerGroup([]string{"localhost:9092"}, "osef", config)
	if err != nil {
		panic(errors.Wrap(err, "failed to create consummer"))
	}

	err = kc.Consume(ctx, []string{"plouf"}, &consumer{})
	if err != nil {
		panic(errors.Wrap(err, "failed to consume topic plouf"))
	}

	time.Sleep(time.Minute)
}

type consumer struct{}

func (c consumer) Setup(session sarama.ConsumerGroupSession) error {
	fmt.Println("Starting to consume", session.Claims())

	return nil
}

func (c consumer) Cleanup(session sarama.ConsumerGroupSession) error {
	fmt.Println("done consuming", session.Claims())

	return nil
}

func (c consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		fmt.Println("received", string(message.Value))
	}

	return nil
}
