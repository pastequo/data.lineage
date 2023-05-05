package main

import (
	"encoding/json"
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
)

type Data struct {
	ID      string   `json:"id"`
	Parents []string `json:"parents"`
}

func main() {
	config := sarama.NewConfig()
	config.Version = sarama.V3_3_1_0
	config.Producer.Return.Successes = true

	kp, err := sarama.NewSyncProducer([]string{"localhost:9092"}, config)
	if err != nil {
		panic(errors.Wrap(err, "failed to create sync producer"))
	}

	for i := 0; i < 10; i++ {
		data := Data{
			ID: fmt.Sprintf("data-%d", i),
			Parents: []string{
				fmt.Sprintf("data-%d", i-1),
			},
		}

		dataStr, err := json.Marshal(data)
		if err != nil {
			panic(errors.Wrap(err, "failed to marshal message"))
		}

		partition, offset, err := kp.SendMessage(&sarama.ProducerMessage{
			Topic: "lineage",
			Value: sarama.StringEncoder(dataStr),
		})
		if err != nil {
			panic(errors.Wrap(err, "failed to push message"))
		}

		fmt.Println("Successfully send message to", partition, offset)
	}
}
