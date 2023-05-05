package main

import (
	"context"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Country struct {
	Name string

	MinLat float32
	MaxLat float32

	MinLon float32
	MaxLon float32
}

func main() {
	ctx := context.Background()

	client, err := mongo.NewClient(&options.ClientOptions{
		Hosts: []string{"localhost:27017"},
	})
	if err != nil {
		panic(errors.Wrap(err, "failed to create mongo client"))
	}

	err = client.Connect(ctx)
	if err != nil {
		panic(errors.Wrap(err, "failed to connect mongo client"))
	}

	collection := client.Database("country").Collection("boundingbox")

	_, err = collection.InsertOne(ctx, Country{
		Name: "France",

		MinLat: 43.617207, MinLon: -2.260131,
		MaxLat: 50.281410, MaxLon: 5.590241,
	})
	if err != nil {
		panic(errors.Wrap(err, "failed to insert France"))
	}

	_, err = collection.InsertOne(ctx, Country{
		Name: "Spain",

		MinLat: 36.598213, MinLon: -7.200458,
		MaxLat: 42.334585, MaxLon: 3.087020,
	})
	if err != nil {
		panic(errors.Wrap(err, "failed to insert Spain"))
	}
}
