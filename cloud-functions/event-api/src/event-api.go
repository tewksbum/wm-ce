// Package streamerapi contains a series of cloud functions for streamer
package eventapi

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/datastore"
	"cloud.google.com/go/pubsub"
	"github.com/google/uuid"
)

// Customer contains Customer fields
type Customer struct {
	Name        string
	AccessKey   string
	Enabled     bool
	Owner       string
	Key         *datastore.Key `datastore:"__key__"`
	CreatedBy   *datastore.Key
	Permissions []string
}

type Event struct {
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

type Signature struct {
	OwnerID   string `json:"ownerId"`
	Source    string `json:"source"`
	EventID   string `json:"eventId"`
	EventType string `json:"eventType"`
}

type Output struct {
	Signature   Signature              `json:"signature"`
	Passthrough map[string]string      `json:"passthrough"`
	Attributes  map[string]string      `json:"attributes"`
	EventData   map[string]interface{} `json:"eventData"`
}

type KVP struct {
	Key   string `json:"k" datastore:"k"`
	Value string `json:"v" datastore:"v"`
}

// ProjectID is the env var of project id
var ProjectID = os.Getenv("PROJECTID")

// DSProjectID is the env var of project id
var DSProjectID = os.Getenv("DSPROJECTID")

// NameSpace is the env var for datastore name space of streamer
var NameSpace = os.Getenv("DATASTORENS")

var PubSubTopic = os.Getenv("PSOUTPUT")

// global vars
var ctx context.Context
var ps *pubsub.Client
var topic *pubsub.Topic

var FileUrlKey = "fileUrl"

func init() {
	ctx = context.Background()
	ps, _ = pubsub.NewClient(ctx, ProjectID)
	topic = ps.Topic(PubSubTopic)
	log.Printf("init completed, pubsub topic names: %v", topic)
}

// ProcessEvent Receives a http event request
func ProcessEvent(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Owner       string                 `json:"owner"`
		Source      string                 `json:"source"`
		AccessKey   string                 `json:"accessKey"`
		Passthrough map[string]string      `json:"passthrough"`
		Attributes  map[string]string      `json:"attributes"`
		EventID     string                 `json:"eventId"`
		EventType   string                 `json:"eventType"`
		EventData   map[string]interface{} `json:"eventData"`
	}

	if r.Method == http.MethodOptions {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		w.Header().Set("Access-Control-Max-Age", "3600")
		w.WriteHeader(http.StatusNoContent)
		return
	}
	// Set CORS headers for the main request.
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")

	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "{\"success\": false, \"message\": \"Error decoding request\"}")
		log.Fatalf("error decoding request %v", err)
		return
	}

	// validate key
	dsClient, err := datastore.NewClient(ctx, ProjectID)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		log.Fatalf("Error accessing datastore: %v", err)
		fmt.Fprint(w, "{\"success\": false, \"message\": \"Internal error occurred, -1\"}")
		return
	}
	var entities []Customer
	query := datastore.NewQuery("Customer").Namespace(NameSpace).Filter("AccessKey =", input.AccessKey).Limit(1)

	if _, err := dsClient.GetAll(ctx, query, &entities); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		log.Fatalf("Error querying customer: %v", err)
		fmt.Fprint(w, "{\"success\": false, \"message\": \"Internal error occurred, -2\"}")
		return
	}
	if len(entities) == 0 {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprint(w, "{\"success\": false, \"message\": \"Invalid access key, -10\"}")
		return
	}

	customer := entities[0]
	if customer.Enabled == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprint(w, "{\"success\": false, \"message\": \"Account is not enabled, -11\"}")
		return
	}

	if !strings.EqualFold(customer.Owner, input.Owner) {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprint(w, "{\"success\": false, \"message\": \"Invalid credentials, -9\"}")
		return
	}

	if len(input.EventID) == 0 {
		input.EventID = uuid.New().String()
		log.Printf("event assingned id %v", input.EventID)
	} else {
		log.Printf("event supplied with id %v", input.EventID)
	}

	if len(input.EventType) == 0 {
		input.EventType = "Unknown"
		log.Printf("event assingned type %v", input.EventType)
	} else {
		log.Printf("event supplied with type %v", input.EventType)
	}

	// log the request
	OwnerKey := customer.Key.Name
	if len(OwnerKey) == 0 {
		OwnerKey = strconv.FormatInt(customer.Key.ID, 10)
	}
	event := &Event{
		CustomerID:  OwnerKey,
		Created:     time.Now(),
		Source:      input.Source,
		Owner:       input.Owner,
		Passthrough: ToKVPSlice(&input.Passthrough),
		Attributes:  ToKVPSlice(&input.Attributes),
		EventID:     input.EventID,
		EventType:   input.EventType,
		Endpoint:    "EVENT",
	}

	fsClient, err := datastore.NewClient(ctx, DSProjectID)
	eventKey := datastore.IncompleteKey("Event", nil)
	eventKey.Namespace = NameSpace
	if _, err := fsClient.Put(ctx, eventKey, event); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		log.Fatalf("Error logging event: %v", err)
		fmt.Fprint(w, "{\"success\": false, \"message\": \"Internal error occurred, -3\"}")
		return
	}
	fmt.Fprintf(w, "{\"success\": true, \"message\": \"Request queued\", \"id\": \"%v\"}", input.EventID)

	var output Output
	output.Passthrough = input.Passthrough
	output.Attributes = input.Attributes
	output.EventData = input.EventData
	output.Signature = Signature{
		OwnerID:   OwnerKey,
		Source:    input.Source,
		EventID:   input.EventID,
		EventType: input.EventType,
	}

	outputJSON, _ := json.Marshal(output)

	// this is a data request, drop to eventdata pubsub
	psresult := topic.Publish(ctx, &pubsub.Message{
		Data: outputJSON,
	})

	psid, err := psresult.Get(ctx)
	_, err = psresult.Get(ctx)
	if err != nil {
		log.Fatalf("%v Could not pub to pubsub: %v", output.Signature.EventID, err)
	} else {
		log.Printf("%v pubbed record as message id %v: %v", output.Signature.EventID, psid, string(outputJSON))
	}
}

func ToKVPSlice(v *map[string]string) []KVP {
	var result []KVP
	for k, v := range *v {
		result = append(result, KVP{
			Key:   k,
			Value: v,
		})
	}
	return result
}
