package filestatus

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/datastore"
	"cloud.google.com/go/pubsub"
)

// PubSubMessage is the payload of a pubsub event
type PubSubMessage struct {
	Data []byte `json:"data"`
}

type Signature struct {
	OwnerID   string `json:"ownerId"`
	Source    string `json:"source"`
	EventID   string `json:"eventId"`
	EventType string `json:"eventType"`
	RecordID  string `json:"recordId"`
	RowNumber int    `json:"rowNum"`
}

type Input struct {
	Signature   Signature              `json:"signature"`
	Passthrough map[string]string      `json:"passthrough"`
	Attributes  map[string]string      `json:"attributes"`
	EventData   map[string]interface{} `json:"eventData"`
}

type Event struct {
	Key         *datastore.Key `datastore:"__key__"`
	CustomerID  string
	Owner       string
	EventID     string
	EventType   string
	Source      string
	Status      string
	Message     string
	Created     time.Time
	Endpoint    string
	Passthrough []KVP
	Attributes  []KVP
	Detail      string
}

type KVP struct {
	Key   string `json:"k" datastore:"k"`
	Value string `json:"v" datastore:"v"`
}

// ProjectID is the env var of project id
var ProjectID = os.Getenv("PROJECTID")
var PubSubTopic = os.Getenv("PSOUTPUT")
var NameSpace = os.Getenv("DATASTORENS")
var MaxRepetition = 12 * 24 // wait for at most 24 hour for a file to finish processing
var Environment = os.Getenv("ENVIRONMENT")

// global vars
var ctx context.Context
var ps *pubsub.Client
var topic *pubsub.Topic
var ds *datastore.Client

func init() {
	ctx = context.Background()
	ps, _ = pubsub.NewClient(ctx, ProjectID)
	ds, _ = datastore.NewClient(ctx, ProjectID)
	topic = ps.Topic(PubSubTopic)

	log.Printf("init completed, pubsub topic name: %v", topic)
}

/*CheckStatus receives status messages from file processor and checks for completion of processing
 */
func CheckStatus(ctx context.Context, m PubSubMessage) error {
	var input Input
	if err := json.Unmarshal(m.Data, &input); err != nil {
		log.Fatalf("Unable to unmarshal message %v with error %v", string(m.Data), err)
	}
	log.Printf("Received message %v", string(m.Data))
	// get the file
	if status, ok := input.EventData["success"]; ok {
		if status == true {
			if val, ok := input.EventData["runcount"]; ok {
				// we've waited for at least 5 min now, we can process
				runCount := int(val.(float64))
				recordCompleted := false
				fiberCompleted := false
				recordCount := int(input.EventData["recordcount"].(float64))
				var recordIDs []datastore.Key
				var peopleFiberRecordIDs []string
				var peoplrFiberRecordIDsNormalized []string

				OwnerNamespace := strings.ToLower(fmt.Sprintf("%v-%v", Environment, strings.ToLower(input.Signature.OwnerID)))

				if _, err := ds.GetAll(ctx, datastore.NewQuery("record").Namespace(OwnerNamespace).Filter("EventID =", input.Signature.EventID).KeysOnly(), &recordIDs); err != nil {
					log.Printf("Error querying records: %v", err)
				}
				if _, err := ds.GetAll(ctx, datastore.NewQuery("people-fiber").Namespace(OwnerNamespace).Filter("eventid =", input.Signature.EventID).Project("recordid").Distinct(), &peopleFiberRecordIDs); err != nil {
					log.Printf("Error querying records: %v", err)
				}
				for _, k := range peopleFiberRecordIDs {
					recordID := Left(k, 36)
					if !Contains(peoplrFiberRecordIDsNormalized, recordID) {
						peoplrFiberRecordIDsNormalized = append(peoplrFiberRecordIDsNormalized, recordID)
					}
				}

				completed := recordCompleted && fiberCompleted
				if runCount >= MaxRepetition || completed {
					// write the status
					var requests []Event
					var request Event
					query := datastore.NewQuery("Event").Namespace(NameSpace).Filter("EventID =", input.Signature.EventID).Limit(1)
					if _, err := ds.GetAll(ctx, query, &requests); err != nil {
						log.Fatalf("Error querying event: %v", err)
						return nil
					} else if len(requests) > 0 {
						request = requests[0]
						if completed {
							request.Status = "Completed"
							request.Message = fmt.Sprintf("processed %v rows, %v records, %v fibers", recordCount, len(recordIDs), len(peoplrFiberRecordIDsNormalized))
						} else {
							request.Status = "Warning"
							request.Message = "%v rows produced %v records and %v fibers"
						}
						if _, err := ds.Put(ctx, request.Key, request); err != nil {
							log.Fatalf("error updating event: %v", err)
						}
					}

				} else {
					// sleep for 5 min and push this message back out
					input.EventData["runcount"] = int(input.EventData["runcount"].(float64)) + 1
					statusJSON, _ := json.Marshal(input)
					time.Sleep(300 * time.Second)
					psresult := topic.Publish(ctx, &pubsub.Message{
						Data: statusJSON,
					})
					_, err := psresult.Get(ctx)
					if err != nil {
						log.Fatalf("%v Could not pub to pubsub: %v", input.Signature.EventID, err)
					}
				}

			} else {
				// sleep for 5 min and push this message back out
				input.EventData["runcount"] = 1
				statusJSON, _ := json.Marshal(input)
				time.Sleep(300 * time.Second)
				psresult := topic.Publish(ctx, &pubsub.Message{
					Data: statusJSON,
				})
				_, err := psresult.Get(ctx)
				if err != nil {
					log.Fatalf("%v Could not pub to pubsub: %v", input.Signature.EventID, err)
				}
			}

		} else {
			// if not successful, it's the final status, update the event
			var requests []Event
			var request Event
			query := datastore.NewQuery("Event").Namespace(NameSpace).Filter("EventID =", input.Signature.EventID).Limit(1)
			if _, err := ds.GetAll(ctx, query, &requests); err != nil {
				log.Fatalf("Error querying event: %v", err)
				return nil
			} else if len(requests) > 0 {
				request = requests[0]
				request.Status = "Rejected"
				request.Message = input.EventData["status"].(string)
				if _, err := ds.Put(ctx, request.Key, request); err != nil {
					log.Fatalf("error updating event: %v", err)
				}
			}
		}
	}

	return nil
}

func Left(str string, num int) string {
	if num <= 0 {
		return ``
	}
	if num > len(str) {
		num = len(str)
	}
	return str[:num]
}

func Contains(slice []string, item string) bool {
	for _, v := range slice {
		if strings.EqualFold(v, item) {
			return true
		}
	}
	return false
}
