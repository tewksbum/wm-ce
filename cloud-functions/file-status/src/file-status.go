package filestatus

import (
	"context"
	"encoding/json"
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
	RowLimit    int
	Counters    []KIP
}

type KIP struct {
	Key   string `json:"k" datastore:"k"`
	Value int    `json:"v" datastore:"v"`
}

type KVP struct {
	Key   string `json:"k" datastore:"k"`
	Value string `json:"v" datastore:"v"`
}

// ProjectID is the env var of project id
var ProjectID = os.Getenv("PROJECTID")
var DSProjectID = os.Getenv("DSPROJECTID")
var PubSubTopic = os.Getenv("PSOUTPUT")
var NameSpace = os.Getenv("DATASTORENS")
var MaxRepetition = 12 * 24 // wait for at most 24 hour for a file to finish processing
var Environment = os.Getenv("ENVIRONMENT")

// global vars
var ctx context.Context
var ps *pubsub.Client
var topic *pubsub.Topic
var ds *datastore.Client
var fs *datastore.Client

func init() {
	ctx = context.Background()
	ps, _ = pubsub.NewClient(ctx, ProjectID)
	ds, _ = datastore.NewClient(ctx, ProjectID)
	fs, _ = datastore.NewClient(ctx, DSProjectID)
	topic = ps.Topic(PubSubTopic)
	topic.PublishSettings.DelayThreshold = 5 * time.Minute

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

	var requests []Event
	var request Event

	Status, Message := "", ""
	if val, ok := input.EventData["status"]; ok {
		Status = val.(string)
	}
	if val, ok := input.EventData["message"]; ok {
		Message = val.(string)
	}
	RecordsTotal, RecordsCompleted, RecordsDeleted, FibersCompleted, FibersDeleted, ParentEmails, StudentEmails, CertifiedAddresses, BadAddresses, RowCount := 0, 0, 0, 0, 0, 0, 0, 0, 0, 0

	if val, ok := input.EventData["records-total"]; ok {
		RecordsTotal = int(val.(float64))
	}
	if val, ok := input.EventData["records-completed"]; ok {
		RecordsCompleted = int(val.(float64))
	}
	if val, ok := input.EventData["records-deleted"]; ok {
		RecordsDeleted = int(val.(float64))
	}
	if val, ok := input.EventData["fibers-completed"]; ok {
		FibersCompleted = int(val.(float64))
	}
	if val, ok := input.EventData["fibers-deleted"]; ok {
		FibersDeleted = int(val.(float64))
	}
	if val, ok := input.EventData["parent-emails"]; ok {
		ParentEmails = int(val.(float64))
	}
	if val, ok := input.EventData["student-emails"]; ok {
		StudentEmails = int(val.(float64))
	}
	if val, ok := input.EventData["certified-addresses"]; ok {
		CertifiedAddresses = int(val.(float64))
	}
	if val, ok := input.EventData["bad-addresses"]; ok {
		BadAddresses = int(val.(float64))
	}
	if val, ok := input.EventData["row-count"]; ok {
		RowCount = int(val.(float64))
	}

	query := datastore.NewQuery("Event").Namespace(NameSpace).Filter("EventID =", input.Signature.EventID).Limit(1)
	if _, err := fs.GetAll(ctx, query, &requests); err != nil {
		log.Fatalf("Error querying event: %v", err)
		return nil
	} else if len(requests) > 0 {
		request = requests[0]
		request.Status = Status
		request.Message = Message
		request.Counters = []KIP{
			KIP{Key: "records-total", Value: RecordsTotal},
			KIP{Key: "records-completed", Value: RecordsCompleted},
			KIP{Key: "records-deleted", Value: RecordsDeleted},
			KIP{Key: "fibers-completed", Value: FibersCompleted},
			KIP{Key: "fibers-deleted", Value: FibersDeleted},
			KIP{Key: "parent-emails", Value: ParentEmails},
			KIP{Key: "student-emails", Value: StudentEmails},
			KIP{Key: "certified-addresses", Value: CertifiedAddresses},
			KIP{Key: "bad-addresses", Value: BadAddresses},
			KIP{Key: "row-count", Value: RowCount},
		}
		if _, err := fs.Put(ctx, request.Key, &request); err != nil {
			log.Fatalf("error updating event: %v", err)
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
