package main

import (
	"context"

	arangodb "github.com/arangodb/go-driver"
	"github.com/arangodb/go-driver/http"
	"github.com/pkg/errors"
)

func main() {
	ctx := context.Background()

	// Create an HTTP connection to the database
	conn, err := http.NewConnection(http.ConnectionConfig{
		Endpoints: []string{"http://localhost:8529"},
	})
	if err != nil {
		panic(errors.Wrap(err, "failed to create http connection"))
	}
	// Create a client
	c, err := arangodb.NewClient(arangodb.ClientConfig{
		Connection: conn,
	})
	if err != nil {
		panic(errors.Wrap(err, "failed to create arangodb client"))
	}

	db, err := c.CreateDatabase(ctx, "datalineage", nil)
	if err != nil {
		panic(errors.Wrap(err, "failed to create database"))
	}

	_, err = db.CreateCollection(ctx, "datapoint", &arangodb.CreateCollectionOptions{
		Type: arangodb.CollectionTypeDocument,
	})
	if err != nil {
		panic(errors.Wrap(err, "failed to create collection datapoint"))
	}

	_, err = db.CreateCollection(ctx, "lineage", &arangodb.CreateCollectionOptions{
		Type: arangodb.CollectionTypeEdge,
	})
	if err != nil {
		panic(errors.Wrap(err, "failed to create collection lineage"))
	}

	_, err = db.CreateGraphV2(ctx, "lineage", &arangodb.CreateGraphOptions{
		EdgeDefinitions: []arangodb.EdgeDefinition{
			{
				Collection: "lineage",
				From:       []string{"datapoint"},
				To:         []string{"datapoint"},
			},
		},
	})
	if err != nil {
		panic(errors.Wrap(err, "failed to create graph"))
	}
}
