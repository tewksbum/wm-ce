package orderpipeline

import (
	"context"
	"encoding/json"
	"log"

	"cloud.google.com/go/pubsub"
)

var (
	pubsubTopic = "streamer-output-dev" //"streamer-output-prod"
	outputTopic = "order-output-dev"    // "order-output-prod"
	projectID   = "wemade-core"
)

// Main func for pipeline
func Main(ctx context.Context, m PubSubMessage) error {
	var input InputRecord
	if err := json.Unmarshal(m.Data, &input); err != nil {
		log.Fatal(err)
		return nil
	}

	output, err := pipelineParse(input)
	if err != nil {
		log.Fatalf("Could not parse input data: %v", err)
		return nil
	}

	outputJSON, err := json.Marshal(output)
	if err != nil {
		log.Fatalf("Could not parse output data: %v", err)
		return nil
	}

	log.Printf("output message %v", string(outputJSON))

	psclient, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("Could not create pubsub Client: %v", err)
		return nil
	}
	pstopic := psclient.Topic(outputTopic)
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
