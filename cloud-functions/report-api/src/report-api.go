// Package streamerapi contains a series of cloud functions for streamer
package reportapi

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
	CustomerID string
	Owner      string
	EventID    string
	EventType  string
	Status     string
	Created    time.Time
	Endpoint   string
}

type FileReport struct {
	RequestID   string
	RowCount    int
	ColumnCount int
	Columns     []ColumnStat
	ProcessedOn time.Time
	PcocessTime string
	Counts      TypedCount
}

type OwnerReport struct {
	FileCount int
	Requests  []Event
	Columns   []ColumnStat
	Counts    TypedCount
}

type ColumnStat struct {
	Name     string
	Min      string
	Max      string
	Sparsity float32
	Mapped   []string
}

type TypedCount struct {
	Person        int
	Dupe          int
	Throwaway     int
	HouseHold     int
	International int
	Freshman      int
	Upperclassmen int
}

type Record struct {
	EventType string    `datastore:"Type"`
	EventID   string    `datastore:"EventID"`
	RecordID  string    `datastore:"RecordID"`
	Fields    []KVP     `datastore:"Fields,noindex"`
	TimeStamp time.Time `datastore:"Created"`
}

type KVP struct {
	Key   string `json:"k" datastore:"k"`
	Value string `json:"v" datastore:"v"`
}

// ProjectID is the env var of project id
var ProjectID = os.Getenv("PROJECTID")

// NameSpace is the env var for datastore name space of streamer
var NameSpace = os.Getenv("DATASTORENS")
var Environment = os.Getenv("ENVIRONMENT")
var dev = Environment == "dev"

var DSKRecord = os.Getenv("DSKINDRECORD")
var DSKFiber = os.Getenv("DSKINDFIBER")
var DSKSet = os.Getenv("DSKINDSET")

// global vars
var ctx context.Context
var ds *datastore.Client

func init() {
	ctx = context.Background()
	ds, _ = datastore.NewClient(ctx, ProjectID)
	log.Printf("init completed")
}

// ProcessEvent Receives a http event request
func ProcessRequest(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Owner      string `json:"owner"`
		AccessKey  string `json:"accessKey"`
		ReportType string `json:"reportType"`
		RequestID  string `json:"requestId"`
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

	if !strings.EqualFold(input.ReportType, "file") && !strings.EqualFold(input.ReportType, "owner") {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "{\"success\": false, \"message\": \"reportType must be either file or owner\"}")
		return
	}

	if !strings.EqualFold(input.ReportType, "file") && len(input.RequestID) < 20 {
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
		EventType:  "REPORT",
		Endpoint:   "REPORT",
	}

	OwnerNamespace := strings.ToLower(fmt.Sprintf("%v-%v", Environment, OwnerKey))

	eventKey := datastore.IncompleteKey("Event", nil)
	eventKey.Namespace = NameSpace
	if _, err := ds.Put(ctx, eventKey, event); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		log.Fatalf("Error logging event: %v", err)
		fmt.Fprint(w, "{\"success\": false, \"message\": \"Internal error occurred, -3\"}")
		return
	}

	var output interface{}
	columns := make(map[string]ColumnStat)

	if strings.EqualFold(input.ReportType, "file") {
		report := FileReport{
			RequestID: input.RequestID,
		}
		var records []Record
		RecordsQuery := datastore.NewQuery(DSKRecord).Namespace(OwnerNamespace).Filter("EventID =", input.RequestID)

		if _, err := ds.GetAll(ctx, RecordsQuery, &records); err != nil {
			log.Fatalf("Error querying records: %v", err)
		} else {
			report.RowCount = len(records)
			var minTime time.Time
			var maxTime time.Time
			for i, r := range records {
				if i == 0 {
					minTime = r.TimeStamp
					maxTime = r.TimeStamp
				}
				if r.TimeStamp.After(maxTime) {
					maxTime = r.TimeStamp
				}
				if r.TimeStamp.Before(minTime) {
					minTime = r.TimeStamp
				}
				for _, f := range r.Fields {
					name := strings.ToUpper(f.Key)
					value := strings.TrimSpace(f.Value)
					stat := ColumnStat{Name: name}
					if val, ok := columns[name]; ok {
						stat = val
					}
					if len(value) > 0 {
						stat.Sparsity++
						if len(stat.Min) == 0 || strings.Compare(stat.Min, value) > 0 {
							stat.Min = value
						}
						if len(stat.Max) == 0 || strings.Compare(stat.Max, value) < 0 {
							stat.Max = value
						}
					}

					columns[name] = stat
				}
			}
			report.ColumnCount = len(columns)
			for _, v := range columns {
				report.Columns = append(report.Columns, v)
			}
			report.PcocessTime = fmt.Sprintf("%v s", maxTime.Sub(minTime).Seconds())
			report.ProcessedOn = minTime
		}

		output = report
	} else {
		report := OwnerReport{}

		output = report
	}
	outputJSON, _ := json.Marshal(output)
	fmt.Fprintf(w, string(outputJSON))
}
