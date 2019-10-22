package threesixty

import (
	"context"
	"encoding/json"
	"log"
	"strconv"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/pubsub"
	"github.com/google/uuid"
)

var (
	pubsubTopic = "campaign-output-dev"    //"streamer-output-prod"
	outputTopic = "campaign360-output-dev" // "campaign-output-prod"
	projectID   = "wemade-core"
)

var indexName = "campaign"

// Campaign360 round and around goes the campaign
func Campaign360(ctx context.Context, m PubSubMessage) error {
	log.Println(string(m.Data))
	var input OutputRecord
	if err := json.Unmarshal(m.Data, &input); err != nil {
		log.Fatal(err)
	}

	psclient, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("Could not create pubsub Client: %v", err)
		return nil
	}
	pstopic := psclient.Topic(outputTopic)
	log.Printf("pubsub topic is %v", pstopic)

	// map the data
	var output ThreeSixtyOutputRecord
	output.Owner = input.Owner
	output.Source = input.Source
	output.Request = input.Request
	output.Row = input.Row
	output.TimeStamp = input.TimeStamp
	output.SetID = uuid.New().String()

	output.Campaign = input.Campaign

	// write to BQ
	bqClient, err := bigquery.NewClient(ctx, projectID)

	campaignSchema, err := bigquery.InferSchema(ThreeSixtyOutputRecord{})
	campaignMetaData := &bigquery.TableMetadata{
		Schema: campaignSchema,
	}
	datasetID := strconv.FormatInt(input.Owner, 10)
	tableID := "campaign"
	campaignTableRef := bqClient.Dataset(datasetID).Table(tableID)
	if err := campaignTableRef.Create(ctx, campaignMetaData); err != nil {
		log.Fatalf("error making table %v", err)
		return nil
	}

	campaignInserter := campaignTableRef.Inserter()
	if err := campaignInserter.Put(ctx, output); err != nil {
		log.Fatalf("error insertinng into table %v", err)
		return nil
	}

	log.Printf("%v %v", bqClient, campaignSchema)

	outputJSON, err := json.Marshal(output)
	if err != nil {
		log.Fatalf("Could not parse output data: %v", err)
		return nil
	}

	log.Printf("output message %v", string(outputJSON))

	psresult := pstopic.Publish(ctx, &pubsub.Message{
		Data: outputJSON,
	})
	psid, err := psresult.Get(ctx)
	if err != nil {
		log.Fatalf("Could not pub to pubsub: %v", err)
	} else {
		log.Printf("Published record message id %v", psid)
	}

	return nil
}
