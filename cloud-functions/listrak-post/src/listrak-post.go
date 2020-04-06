package listrakpost

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"

	"cloud.google.com/go/datastore"
	"cloud.google.com/go/pubsub"
)

var ProjectID = os.Getenv("PROJECTID")
var DSProjectID = os.Getenv("DSPROJECTID")
var WMNamespace = os.Getenv("DATASTORENS")
var Env = os.Getenv("ENVIRONMENT")
var dev = Env == "dev"
var ListrakEndpoint = os.Getenv("LISTRAKENDPOINT")

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

func ListrakPost(ctx context.Context, m PubSubMessage) error {
	var input []ContactOutput
	if err := json.Unmarshal(m.Data, &input); err != nil {
		log.Printf("Unable to unmarshal message %v with error %v", string(m.Data), err)
		return nil
	}

	log.Printf("Log PubSubMessage %v", string(m.Data))

	//Authentication  map[string]string{"mostafa": "dahab"}
	data := url.Values{}
	data.Set("grant_type", "client_credentials")
	data.Set("client_id", "g1mukhpg8gkbgrrb1vmz")
	data.Set("client_secret", "xriMvCqzXzewkIgUYuHXL33V08PbTAyUbS/a+NaF/jY")

	req, err := http.NewRequest("POST", ListrakEndpoint, strings.NewReader(data.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded;charset=utf-8")
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("[ERROR] Listrak authentication: %v ", err)
		return nil
	}
	log.Printf("Ok status: %v ", resp.Status)
	defer resp.Body.Close()

	/*
		// look up the event
		var events []Event
		var event Event
		eventQuery := datastore.NewQuery("Event").Namespace(WMNamespace).Filter("EventID =", input.EventID).Limit(1)
		if _, err := fs.GetAll(ctx, eventQuery, &events); err != nil {
			log.Printf("Error querying event: %v", err)
			return nil
		} else if len(events) > 0 {
			event = events[0]
		} else {
			log.Printf("Event ID not found: %v", input.EventID)
			return nil
		}

		// update event
		event.Status = "PROCESSED date:" + time.Now().Format("2006.01.02 15:04:05") + " count:" + input.Count
		log.Printf("EventId: %v message: %v", input.EventID, event.Status)
		if _, err := fs.Put(ctx, event.Key, &event); err != nil {
			log.Printf("error updating event: %v", err)
		}*/
	return nil
}
