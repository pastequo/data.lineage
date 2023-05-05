package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
)

func main() {
	ctx := context.Background()

	config := sarama.NewConfig()
	config.Version = sarama.V3_3_1_0
	config.Producer.Return.Successes = true

	kc, err := sarama.NewConsumerGroup([]string{"localhost:9092"}, "normalizer", config)
	if err != nil {
		panic(errors.Wrap(err, "failed to create consummer"))
	}

	kp, err := sarama.NewSyncProducer([]string{"localhost:9092"}, config)
	if err != nil {
		panic(errors.Wrap(err, "failed to create sync producer"))
	}

	err = kc.Consume(ctx, []string{"raw"}, &consumer{
		kp: kp,
	})
	if err != nil {
		panic(errors.Wrap(err, "failed to consume topic raw"))
	}

	running := make(chan interface{})

	// Start os signal listener.
	interrupter := interrupter{}

	interrupter.handleInterrupt(ctx, running)

	// Waiting for an interruption signal
	select {
	case <-running:
		return
	}
}

type consumer struct {
	kp sarama.SyncProducer
}

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

type interrupter struct {
	interruptChan chan os.Signal
}

func (i interrupter) handleInterrupt(ctx context.Context, running chan interface{}) {
	i.interruptChan = make(chan os.Signal, 1)
	signal.Notify(i.interruptChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		for range i.interruptChan {
			close(running)

			return
		}
	}()
}
