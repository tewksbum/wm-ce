// Package eda contains a Pub/Sub Cloud Function and a big query client that creates a table when its needed.
package eda

import (
	"context"
	"encoding/json"
	"log"
	"os"

	"cloud.google.com/go/bigquery"
)

// ProjectID is the GCP Project ID
var ProjectID = os.Getenv("PROJECTID")

// Dataset is the BigQuery dataset, per client
var Dataset = os.Getenv("DATASET")

// PubSubMessage is the payload of a pubsub event
type PubSubMessage struct {
	Data []byte `json:"data"`
}

type StreamerOutput []struct {
	TrustedID string `json:"TrustedId"`
	Fields    struct {
		AD1   string `json:"AD1"`
		AD2   string `json:"AD2"`
		CITY  string `json:"CITY"`
		EMAIL string `json:"EMAIL"`
		FNAME string `json:"FNAME"`
		LNAME string `json:"LNAME"`
		ST    string `json:"ST"`
		ZIP   string `json:"ZIP"`
		ROLE  string `json:"ROLE"`
	} `json:"Fields"`
	Sources []struct {
		Owner   int64  `json:"Owner"`
		Source  string `json:"Source"`
		Request string `json:"Request"`
		Row     int    `json:"Row"`
	} `json:"Sources"`
}

// Main is the EDA entrypoint of the function, it flattens the json data from pubsub
// and inserts the data into bigquery
func Main(ctx context.Context, m PubSubMessage) error {
	//logs pubsub message
	log.Print(string(m.Data))

	//Flatten json
	var jsonMessage StreamerOutput
	json.Unmarshal(m.Data, &jsonMessage)
	log.Print(jsonMessage)
	log.Print(len(jsonMessage))
	client, err := bigquery.NewClient(ctx, ProjectID)
	if err != nil {
		log.Fatalf("Error connecting to big query: %v", err)
	}
	log.Println(client.Datasets(ctx))
	//Get dataset
	dataset := client.Dataset(Dataset)

	//Get table if it exists,create table if it doesn't
	table := dataset.Table("PEOPLE")
	//Insert flat data into BQ
	inserter := table.Inserter()
	type Item struct {
		City      string
		FirstName string
	}
	items := []*Item{
		// Item implements the ValueSaver interface.
		{City: "Asuncion", FirstName: "Diego"},
		{City: "Asuncion", FirstName: "Hugo"},
	}
	if err := inserter.Put(ctx, items); err != nil {
		return err
	}

	return nil
}
