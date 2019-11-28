// Package streamerapi contains a series of cloud functions for streamer
package reportapi

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"cloud.google.com/go/datastore"
	"github.com/google/uuid"

	"github.com/fatih/structs"
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

type Fiber struct {
	FiberID      *datastore.Key   `datastore:"__key__"`
	CreatedAt    time.Time        `datastore:"createdat"`
	OwnerID      string           `datastore:"ownerid"`
	Source       string           `datastore:"source"`
	EventID      string           `datastore:"eventid"`
	EventType    string           `datastore:"eventtype"`
	RecordID     string           `datastore:"recordid"`
	SALUTATION   MatchKeyField    `datastore:"salutation"`
	NICKNAME     MatchKeyField    `datastore:"nickname"`
	FNAME        MatchKeyField    `datastore:"fname"`
	FINITIAL     MatchKeyField    `datastore:"finitial"`
	LNAME        MatchKeyField    `datastore:"lname"`
	MNAME        MatchKeyField    `datastore:"mname"`
	AD1          MatchKeyField    `datastore:"ad1"`
	AD1NO        MatchKeyField    `datastore:"ad1no"`
	AD2          MatchKeyField    `datastore:"ad2"`
	AD3          MatchKeyField    `datastore:"ad3"`
	CITY         MatchKeyField    `datastore:"city"`
	STATE        MatchKeyField    `datastore:"state"`
	ZIP          MatchKeyField    `datastore:"zip"`
	ZIP5         MatchKeyField    `datastore:"zip5"`
	COUNTRY      MatchKeyField    `datastore:"country"`
	MAILROUTE    MatchKeyField    `datastore:"mailroute"`
	ADTYPE       MatchKeyField    `datastore:"adtype"`
	ADPARSER     MatchKeyField    `datastore:"adparser"`
	ADCORRECT    MatchKeyField    `datastore:"adcorrect"`
	EMAIL        MatchKeyField    `datastore:"email"`
	PHONE        MatchKeyField    `datastore:"phone"`
	TRUSTEDID    MatchKeyField    `datastore:"trustedid"`
	CLIENTID     MatchKeyField    `datastore:"clientid"`
	GENDER       MatchKeyField    `datastore:"gender"`
	AGE          MatchKeyField    `datastore:"age"`
	DOB          MatchKeyField    `datastore:"dob"`
	ORGANIZATION MatchKeyField    `datastore:"organization"`
	TITLE        MatchKeyField    `datastore:"title"`
	ROLE         MatchKeyField    `datastore:"role"`
	STATUS       MatchKeyField    `datastore:"status"`
	Passthrough  []Passthrough360 `datastore:"passthrough"`
}

type PeopleSetMember struct {
	SetID     string
	OwnerID   string
	Source    string
	EventID   string
	EventType string
	RecordID  string
	FiberID   string
}

type HouseholdSetMember struct {
	SetID     string
	OwnerID   string
	Source    string
	EventID   string
	EventType string
	RecordID  string
	FiberID   string
}

type Signature struct {
	OwnerID   string
	Source    string
	EventID   string
	EventType string
	RecordID  string
}

type Passthrough360 struct {
	Name  string
	Value string
}

type MatchKeys struct {
	SALUTATION MatchKeyField
	NICKNAME   MatchKeyField
	FNAME      MatchKeyField
	FINITIAL   MatchKeyField
	LNAME      MatchKeyField
	MNAME      MatchKeyField

	AD1       MatchKeyField
	AD1NO     MatchKeyField
	AD2       MatchKeyField
	AD3       MatchKeyField
	CITY      MatchKeyField
	STATE     MatchKeyField
	ZIP       MatchKeyField
	ZIP5      MatchKeyField
	COUNTRY   MatchKeyField
	MAILROUTE MatchKeyField
	ADTYPE    MatchKeyField
	ADPARSER  MatchKeyField
	ADCORRECT MatchKeyField

	EMAIL MatchKeyField
	PHONE MatchKeyField

	TRUSTEDID MatchKeyField
	CLIENTID  MatchKeyField

	GENDER MatchKeyField
	AGE    MatchKeyField
	DOB    MatchKeyField

	ORGANIZATION MatchKeyField
	TITLE        MatchKeyField
	ROLE         MatchKeyField
	STATUS       MatchKeyField
}

type MatchKeyField struct {
	Value  string
	Source string
	Type   string
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
var DSKSetMember = os.Getenv("DSKINDSETMEMBER")

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
	columnMaps := make(map[string][]string)

	if strings.EqualFold(input.ReportType, "file") {
		report := FileReport{
			RequestID: input.RequestID,
		}
		var records []Record
		var fibers []Fiber
		var setMembers []PeopleSetMember
		var setIDs []string

		if _, err := ds.GetAll(ctx, datastore.NewQuery(DSKRecord).Namespace(OwnerNamespace).Filter("EventID =", input.RequestID), &records); err != nil {
			log.Fatalf("Error querying records: %v", err)
			return
		}
		log.Printf("records retrieved: %v", records)
		if _, err := ds.GetAll(ctx, datastore.NewQuery(DSKFiber).Namespace(OwnerNamespace).Filter("eventid =", input.RequestID), &fibers); err != nil {
			log.Fatalf("Error querying fibers: %v", err)
			return
		}
		log.Printf("fibers retrieved: %v", fibers)
		if _, err := ds.GetAll(ctx, datastore.NewQuery(DSKSetMember).Namespace(OwnerNamespace).Filter("EventID =", input.RequestID), &setMembers); err != nil {
			log.Fatalf("Error querying set members: %v", err)
			return
		}
		log.Printf("set members retrieved: %v", setMembers)
		for _, v := range setMembers {
			if !Contains(setIDs, v.SetID) {
				setIDs = append(setIDs, v.SetID)
			}
		}
		log.Printf("set id retrieved: %v", setIDs)

		MatchKeyNames := structs.Names(&MatchKeys{})

		InternationalCount := 0
		FreshmenCount := 0
		UpperclassmenCount := 0
		recordIDs := []string{}
		CurrentYear := time.Now().Year()
		for _, f := range fibers {
			recordID := Left(f.RecordID, 36)
			if !Contains(recordIDs, recordID) {
				recordIDs = append(recordIDs, recordID)
			}

			for _, m := range MatchKeyNames {
				mk := GetMatchKeyFieldFromFiberByName(&f, m)
				if m == "COUNTRY" {
					country := strings.ToUpper(mk.Value)
					if country != "" && country != "US" && country != "USA" && country != "UNITED STATES" && country != "UNITED STATES OF AMERICA" {
						InternationalCount++
					}
				} else if m == "TITLE" {
					title := mk.Value
					if IsInt(title) {
						class, err := strconv.Atoi(title)
						if err == nil {
							if class == CurrentYear+4 {
								FreshmenCount++
							} else if class >= CurrentYear && class < CurrentYear+4 {
								UpperclassmenCount++
							}
						}
					}

				}
				columnTarget := []string{m}
				if len(mk.Source) > 0 {
					if val, ok := columnMaps[mk.Source]; ok {
						columnTarget = val
						if !Contains(columnTarget, m) {
							columnTarget = append(columnTarget, m)
						}
					}
					columnMaps[strings.ToUpper(mk.Source)] = columnTarget
				}
			}
		}
		log.Printf("column maps: %v", columnMaps)

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
			v.Mapped = columnMaps[v.Name]
			report.Columns = append(report.Columns, v)
		}

		sort.Slice(report.Columns, func(i, j int) bool {
			return strings.Compare(report.Columns[i].Name, report.Columns[j].Name) > 0
		})

		report.PcocessTime = fmt.Sprintf("%v s", maxTime.Sub(minTime).Seconds())
		report.ProcessedOn = minTime

		report.Counts = TypedCount{
			Person:        len(setIDs),
			Dupe:          len(fibers) - len(setIDs),
			Throwaway:     len(records) - len(recordIDs), // unique record id, take first 36 characters of record id, to avoid counting MPR records
			HouseHold:     0,
			International: InternationalCount,
			Freshman:      FreshmenCount,
			Upperclassmen: UpperclassmenCount,
		}
		output = report
	} else {
		report := OwnerReport{}

		output = report
	}
	outputJSON, _ := json.Marshal(output)
	fmt.Fprintf(w, string(outputJSON))
}

func GetMatchKeyFieldFromFiberByName(v *Fiber, field string) MatchKeyField {
	r := reflect.ValueOf(v)
	f := reflect.Indirect(r).FieldByName(field)
	return f.Interface().(MatchKeyField)
}

func Contains(slice []string, item string) bool {
	for _, v := range slice {
		if strings.EqualFold(v, item) {
			return true
		}
	}
	return false
}

func IsInt(s string) bool {
	for _, c := range s {
		if !unicode.IsDigit(c) {
			return false
		}
	}
	return true
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
