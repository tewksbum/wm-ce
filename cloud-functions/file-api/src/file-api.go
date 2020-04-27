// Package streamerapi contains a series of cloud functions for streamer
package fileapi

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
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

type FileReport struct {
	ID              string    `json:"id,omitempty"`
	RequestedAt     time.Time `json:"requestedAt,omitempty"`
	ProcessingBegin time.Time `json:"processingBegin,omitempty"`
	ProcessingEnd   time.Time `json:"processingEnd,omitempty"`
	Attributes      []KVP     `json:"attributes,omitempty"`
	Passthroughs    []KVP     `json:"passthroughs,omitempty"`
	CustomerID      string    `json:"customerId,omitempty"`
	InputFilePath   string    `json:"inputFilePath,omitempty"`
	InputFileName   string    `json:"inputFileName,omitempty"`
	Owner           string    `json:"owner,omitempty"`
	StatusLabel     string    `json:"statusLabel,omitempty"`
	StatusBy        string    `json:"statusBy,omitempty"`
}

// ProjectID is the env var of project id
var ProjectID = os.Getenv("PROJECTID")
var DSProjectID = os.Getenv("DSPROJECTID")

// NameSpace is the env var for datastore name space of streamer
var NameSpace = os.Getenv("DATASTORENS")

// global vars
var ctx context.Context
var ps *pubsub.Client
var topic *pubsub.Topic
var topicR *pubsub.Topic
var cfName = os.Getenv("FUNCTION_NAME")

func init() {
	ctx = context.Background()
	ps, _ = pubsub.NewClient(ctx, ProjectID)
	topic = ps.Topic(os.Getenv("PSOUTPUT"))
	topicR = ps.Topic(os.Getenv("PSREPORT"))
	log.Printf("init completed, pubsub topic names: %v", topic)
}

// ProcessEvent Receives a http event request
func ProcessEvent(w http.ResponseWriter, r *http.Request) {
	log.Printf("Invoked by IP %v with headers %v", r.RemoteAddr, r.Header)
	var input struct {
		Owner       string            `json:"owner"`
		Source      string            `json:"source"`
		AccessKey   string            `json:"accessKey"`
		Passthrough map[string]string `json:"passthrough"`
		Attributes  map[string]string `json:"attributes"`
		FileURL     string            `json:"fileUrl"`
		MaxRows     int               `json:"maxRows"`
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
		fmt.Fprint(w, "{success: false, message: \"Error decoding request\"}")
		log.Fatalf("error decoding request %v", err)
		return
	}

	// validate key
	dsClient, err := datastore.NewClient(ctx, ProjectID)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		log.Fatalf("Error accessing datastore: %v", err)
		fmt.Fprint(w, "{success: false, message: \"Internal error occurred, -1\"}")
		return
	}
	var entities []Customer
	query := datastore.NewQuery("Customer").Namespace(NameSpace).Filter("AccessKey =", input.AccessKey).Limit(1)

	if _, err := dsClient.GetAll(ctx, query, &entities); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		log.Fatalf("Error querying customer: %v", err)
		fmt.Fprint(w, "{success: false, message: \"Internal error occurred, -2\"}")
		return
	}
	if len(entities) == 0 {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprint(w, "{success: false, message: \"Invalid access key, -10\"}")
		return
	}

	log.Printf("found %v matches: %v", len(entities), entities)

	customer := entities[0]
	if customer.Enabled == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprint(w, "{success: false, message: \"Account is not enabled, -11\"}")
		return
	}

	if !strings.EqualFold(customer.Owner, input.Owner) {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprint(w, "{success: false, message: \"Invalid credentials, -9\"}")
		return
	}

	if len(input.Source) == 0 {
		input.Source = "Default"
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
		EventID:     uuid.New().String(),
		Status:      "Pending",
		EventType:   "UPLOAD",
		Endpoint:    "FILE",
		Detail:      input.FileURL,
		RowLimit:    input.MaxRows,
	}

	report := FileReport{
		ID:            event.EventID,
		RequestedAt:   event.Created,
		Attributes:    event.Attributes,
		Passthroughs:  event.Passthrough,
		CustomerID:    event.CustomerID,
		InputFilePath: event.Detail,
		InputFileName: fileName,
		Owner:         event.Owner,
		StatusLabel:   "request received",
		StatusBy:      cfName,
	}
	reportJSON, _ := json.Marshal(report)
	reportPub := topicR.Publish(ctx, &pubsub.Message{
		Data: reportJSON,
		Attributes: map[string]string{
			"source": cfName,
		},
	})
	_, err = reportPub.Get(ctx)
	if err != nil {
		log.Printf("ERROR %v Could not pub to reporting pubsub: %v", output.Signature.EventID, err)
	}

	fsClient, err := datastore.NewClient(ctx, DSProjectID)
	eventKey := datastore.IncompleteKey("Event", nil)
	eventKey.Namespace = NameSpace
	if _, err := fsClient.Put(ctx, eventKey, event); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		log.Fatalf("Error logging event: %v", err)
		fmt.Fprint(w, "{success: false, message: \"Internal error occurred, -3\"}")
		return
	}
	fmt.Fprintf(w, "{\"success\": true, \"message\": \"Request queued\", \"id\": \"%v\"}", event.EventID)

	var output Output
	output.Passthrough = input.Passthrough
	output.Attributes = input.Attributes
	output.EventData = make(map[string]interface{})
	output.EventData["fileUrl"] = input.FileURL
	output.EventData["maxRows"] = input.MaxRows
	output.Signature = Signature{
		OwnerID:   OwnerKey,
		Source:    input.Source,
		EventID:   event.EventID,
		EventType: event.EventType,
	}

	outputJSON, _ := json.Marshal(output)

	// this is a file request
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

	// populate report
	if len(cfName) == 0 {
		cfName = "file-api"
	}
	fileName := ""
	fileURL, err := url.Parse(event.Detail)
	if err != nil {
	} else {
		fileName = filepath.Base(fileURL.Path)
	}

}

func ToJson(v *map[string]string) string {
	jsonString, err := json.Marshal(v)
	if err == nil {
		return string(jsonString)
	} else {
		log.Fatalf("%v Could not convert map %v to json: %v", v, err)
		return ""
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
