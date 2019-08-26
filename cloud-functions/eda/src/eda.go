// Package eda contains a Pub/Sub Cloud Function and a big query client that creates a table when its needed.
package eda

import (
	"context"
	"log"
	"os"

	"cloud.google.com/go/bigquery"
)

// ProjectID is the GCP Project ID
var ProjectID = os.Getenv("PROJECTID")

// PubSubMessage is the payload of a pubsub event
type PubSubMessage struct {
	Data []byte `json:"data"`
}

// Main is the EDA entrypoint of the function, it flattens the json data from pubsub
// and inserts the data into bigquery
func Main(ctx context.Context, m PubSubMessage) error {
	//logs pubsub message
	log.Println(string(m.Data))

	client, err := bigquery.NewClient(ctx, ProjectID)
	if err != nil {
		log.Fatalf("Error connecting to big query: %v", err)
	}
	log.Println(client.Datasets(ctx))
	//Flatten json
	//Get data-set
	//Get table if it exists,create table if it doesn't
	//Insert flat data into BQ
	return nil
}
