// Package statusapi contains a series of cloud functions for streamer
package statusapi

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

type FileStatus struct {
	RequestID   string
	SubmittedOn time.Time
	Status      string
	Message     string
}

type KVP struct {
	Key   string `json:"k" datastore:"k"`
	Value string `json:"v" datastore:"v"`
}

// ProjectID is the env var of project id
var ProjectID = os.Getenv("PROJECTID")
var DSProjectID = os.Getenv("DSPROJECTID")

// NameSpace is the env var for datastore name space of streamer
var NameSpace = os.Getenv("DATASTORENS")
var Environment = os.Getenv("ENVIRONMENT")

// global vars
var ctx context.Context
var ds *datastore.Client
var fs *datastore.Client

func init() {
	ctx = context.Background()
	ds, _ = datastore.NewClient(ctx, ProjectID)
	fs, _ = datastore.NewClient(ctx, DSProjectID)
	log.Printf("init completed")
}

// ProcessRequest Receives a http event request
func ProcessRequest(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Owner     string `json:"owner"`
		AccessKey string `json:"accessKey"`
		RequestID string `json:"requestId"`
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
	var entities []Customer
	query := datastore.NewQuery("Customer").Namespace(NameSpace).Filter("AccessKey =", input.AccessKey).Limit(1)

	if _, err := ds.GetAll(ctx, query, &entities); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		log.Fatalf("Error querying customer: %v", err)
		fmt.Fprint(w, "{\"success\": false, \"message\": \"Internal error occurred, -2\"}")
		return
	}
	if len(entities) == 0 {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprint(w, "{\"success\": false, \"message\": \"Invalid access key, -10\"}")
		return
	} else {
		log.Printf("found %v matches: %v", len(entities), entities)
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

	if len(input.RequestID) < 36 {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "{\"success\": false, \"message\": \"requestId must be supplied for reportType of file\"}")
		return
	}

	// log the request
	OwnerKey := customer.Key.Name
	if len(OwnerKey) == 0 {
		OwnerKey = strconv.FormatInt(customer.Key.ID, 10)
	}

	event := &Event{
		CustomerID: OwnerKey,
		Created:    time.Now(),
		Owner:      input.Owner,
		EventID:    uuid.New().String(),
		EventType:  "STATUS",
		Endpoint:   "STATUS",
	}

	eventKey := datastore.IncompleteKey("Event", nil)
	eventKey.Namespace = NameSpace
	if _, err := fs.Put(ctx, eventKey, event); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		log.Fatalf("Error logging event: %v", err)
		fmt.Fprint(w, "{\"success\": false, \"message\": \"Internal error occurred, -3\"}")
		return
	}

	report := FileStatus{
		RequestID: input.RequestID,
	}
	var requests []Event
	var request Event
	eventQuery := datastore.NewQuery("Event").Namespace(NameSpace).Filter("EventID =", input.RequestID).Limit(1)
	if _, err := fs.GetAll(ctx, eventQuery, &requests); err != nil {
		log.Fatalf("Error querying event: %v", err)
		return
	} else if len(requests) > 0 {
		request = requests[0]
		report.Status = request.Status
		report.Message = request.Message
		report.SubmittedOn = request.Created

	}

	outputJSON, err := json.Marshal(report)
	if err != nil {
		log.Fatalf("Error writing json %v", err)
	}
	fmt.Fprintf(w, string(outputJSON))
}
