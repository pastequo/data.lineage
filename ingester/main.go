package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	"github.com/arangodb/go-driver"
	"github.com/arangodb/go-driver/http"
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

	datapoint, lineage := newArangoDBClient()

	err = kc.Consume(ctx, []string{"lineage"}, &consumer{datapoint, lineage})
	if err != nil {
		panic(errors.Wrap(err, "failed to consume topic lineage"))
	}

	time.Sleep(time.Hour)
}

func newArangoDBClient() (driver.Collection, driver.Collection) {
	ctx := context.Background()

	// Create an HTTP connection to the database
	conn, err := http.NewConnection(http.ConnectionConfig{
		Endpoints: []string{"http://localhost:8529"},
	})
	if err != nil {
		panic(errors.Wrap(err, "failed to create http connection"))
	}
	// Create a client
	c, err := driver.NewClient(driver.ClientConfig{
		Connection: conn,
	})
	if err != nil {
		panic(errors.Wrap(err, "failed to create arangodb client"))
	}

	db, err := c.Database(ctx, "datalineage")
	if err != nil {
		panic(errors.Wrap(err, "failed to get db datalineage"))
	}

	datapoint, err := db.Collection(ctx, "datapoint")
	if err != nil {
		panic(errors.Wrap(err, "failed to get collection datapoint"))
	}

	lineage, err := db.Collection(ctx, "lineage")
	if err != nil {
		panic(errors.Wrap(err, "failed to get collection lineage"))
	}

	return datapoint, lineage
}

type consumer struct {
	datapoint driver.Collection
	lineage   driver.Collection
}

func (c consumer) Setup(session sarama.ConsumerGroupSession) error {
	fmt.Println("Starting to consume", session.Claims())

	return nil
}

func (c consumer) Cleanup(session sarama.ConsumerGroupSession) error {
	fmt.Println("done consuming", session.Claims())

	return nil
}

type Data struct {
	ID      string   `json:"id"`
	Parents []string `json:"parents"`
}

type Datapoint struct {
	Key string `json:"_key"`
}

type Lineage struct {
	From string `json:"_from"`
	To   string `json:"_to"`
}

func (c consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		fmt.Println("received", string(message.Value))

		data := Data{}

		err := json.Unmarshal(message.Value, &data)
		if err != nil {
			panic(errors.Wrap(err, "failed to process message"))
		}

		_, err = c.datapoint.CreateDocument(context.Background(), Datapoint{Key: data.ID})
		if err != nil {
			panic(errors.Wrap(err, "failed to create datapoint"))
		}

		for _, parent := range data.Parents {
			exist, err := c.datapoint.DocumentExists(context.Background(), parent)
			if err != nil {
				panic(errors.Wrap(err, "failed to check if document exists"))
			}

			if !exist {
				_, err = c.datapoint.CreateDocument(context.Background(), Datapoint{Key: parent})
				if err != nil {
					panic(errors.Wrap(err, "failed to create parent datapoint"))
				}
			}

			_, err = c.lineage.CreateDocument(context.Background(), Lineage{
				From: fmt.Sprintf("datapoint/%s", parent),
				To:   fmt.Sprintf("datapoint/%s", data.ID),
			})

			if err != nil {
				panic(errors.Wrap(err, "failed to create lineage"))
			}
		}
	}

	return nil
}
