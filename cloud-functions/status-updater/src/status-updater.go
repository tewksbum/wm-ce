package statusupdater

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"time"

	"cloud.google.com/go/datastore"
	"cloud.google.com/go/pubsub"
)

var ProjectID = os.Getenv("PROJECTID")
var DSProjectID = os.Getenv("DSPROJECTID")
var WMNamespace = os.Getenv("DATASTORENS")
var Env = os.Getenv("ENVIRONMENT")
var dev = Env == "dev"

var ps *pubsub.Client
var topic *pubsub.Topic
var ds *datastore.Client
var fs *datastore.Client

// var setSchema bigquery.Schema

func init() {
	ctx := context.Background()
	ps, _ = pubsub.NewClient(ctx, ProjectID)
	ds, _ = datastore.NewClient(ctx, ProjectID)
	fs, _ = datastore.NewClient(ctx, DSProjectID)
}

func StatusUpdater(ctx context.Context, m PubSubMessage) error {
	var input Input
	if err := json.Unmarshal(m.Data, &input); err != nil {
		log.Fatalf("Unable to unmarshal message %v with error %v", string(m.Data), err)
	}

	log.Printf("Log PubSubMessage", string(m.Data))

	// look up the event
	var events []Event
	var event Event
	eventQuery := datastore.NewQuery("Event").Namespace(WMNamespace).Filter("EventID =", input.EventID).Limit(1)
	if _, err := fs.GetAll(ctx, eventQuery, &events); err != nil {
		log.Fatalf("Error querying event: %v", err)
		return nil
	} else if len(events) > 0 {
		event = events[0]
	} else {
		log.Fatalf("Event ID not found: %v", input.EventID)
		return nil
	}
	// update event
	event.Status = "Uploaded in CP on " + time.Now().Format("2006.01.02 15:04:05")
	if _, err := fs.Put(ctx, event.Key, &event); err != nil {
		log.Fatalf("error updating event: %v", err)
	}
	return nil
}
