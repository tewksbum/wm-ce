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
	ID           *datastore.Key   `datastore:"__key__"`
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
	ADBOOK       MatchKeyField    `datastore:"adbook"`
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

type PeopleMatchKeys struct {
	SALUTATION   MatchKeyField
	NICKNAME     MatchKeyField
	FNAME        MatchKeyField
	FINITIAL     MatchKeyField
	LNAME        MatchKeyField
	MNAME        MatchKeyField
	AD1          MatchKeyField
	AD1NO        MatchKeyField
	AD2          MatchKeyField
	AD3          MatchKeyField
	CITY         MatchKeyField
	STATE        MatchKeyField
	ZIP          MatchKeyField
	ZIP5         MatchKeyField
	COUNTRY      MatchKeyField
	MAILROUTE    MatchKeyField
	ADTYPE       MatchKeyField
	ADBOOK       MatchKeyField
	ADPARSER     MatchKeyField
	ADCORRECT    MatchKeyField
	EMAIL        MatchKeyField
	PHONE        MatchKeyField
	TRUSTEDID    MatchKeyField
	CLIENTID     MatchKeyField
	GENDER       MatchKeyField
	AGE          MatchKeyField
	DOB          MatchKeyField
	ORGANIZATION MatchKeyField
	TITLE        MatchKeyField
	ROLE         MatchKeyField
	STATUS       MatchKeyField
}

type HouseHoldMatchKeys struct {
	LNAME   MatchKeyField
	CITY    MatchKeyField
	STATE   MatchKeyField
	ZIP     MatchKeyField
	ZIP5    MatchKeyField
	COUNTRY MatchKeyField
	AD1     MatchKeyField
	AD1NO   MatchKeyField
	AD2     MatchKeyField
	ADTYPE  MatchKeyField
	ADBOOK  MatchKeyField
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

type PeopleSet struct {
	ID                     *datastore.Key `datastore:"__key__"`
	OwnerID                []string       `datastore:"ownerid"`
	Source                 []string       `datastore:"source"`
	EventID                []string       `datastore:"eventid"`
	EventType              []string       `datastore:"eventtype"`
	RecordID               []string       `datastore:"recordid"`
	RecordIDNormalized     []string       `datastore:"recordidnormalized"`
	CreatedAt              time.Time      `datastore:"createdat"`
	Fibers                 []string       `datastore:"fibers"`
	SALUTATION             []string       `datastore:"salutation"`
	SALUTATIONNormalized   []string       `datastore:"salutationnormalized"`
	NICKNAME               []string       `datastore:"nickname"`
	NICKNAMENormalized     []string       `datastore:"nicknamenormalized"`
	FNAME                  []string       `datastore:"fname"`
	FNAMENormalized        []string       `datastore:"fnamenormalized"`
	FINITIAL               []string       `datastore:"finitial"`
	FINITIALNormalized     []string       `datastore:"finitialnormalized"`
	LNAME                  []string       `datastore:"lname"`
	LNAMENormalized        []string       `datastore:"lnamenormalized"`
	MNAME                  []string       `datastore:"mname"`
	MNAMENormalized        []string       `datastore:"mnamenormalized"`
	AD1                    []string       `datastore:"ad1"`
	AD1Normalized          []string       `datastore:"ad1normalized"`
	AD1NO                  []string       `datastore:"ad1no"`
	AD1NONormalized        []string       `datastore:"ad1nonormalized"`
	AD2                    []string       `datastore:"ad2"`
	AD2Normalized          []string       `datastore:"ad2normalized"`
	AD3                    []string       `datastore:"ad3"`
	AD3Normalized          []string       `datastore:"ad3normalized"`
	CITY                   []string       `datastore:"city"`
	CITYNormalized         []string       `datastore:"citynormalized"`
	STATE                  []string       `datastore:"state"`
	STATENormalized        []string       `datastore:"statenormalized"`
	ZIP                    []string       `datastore:"zip"`
	ZIPNormalized          []string       `datastore:"zipnormalized"`
	ZIP5                   []string       `datastore:"zip5"`
	ZIP5Normalized         []string       `datastore:"zip5normalized"`
	COUNTRY                []string       `datastore:"country"`
	COUNTRYNormalized      []string       `datastore:"countrynormalized"`
	MAILROUTE              []string       `datastore:"mailroute"`
	MAILROUTENormalized    []string       `datastore:"mailroutenormalized"`
	ADTYPE                 []string       `datastore:"adtype"`
	ADTYPENormalized       []string       `datastore:"adtypenormalized"`
	ADBOOK                 []string       `datastore:"adbook"`
	ADBOOKNormalized       []string       `datastore:"adbooknormalized"`
	ADPARSER               []string       `datastore:"adparser"`
	ADPARSERNormalized     []string       `datastore:"adparsernormalized"`
	ADCORRECT              []string       `datastore:"adcorrect"`
	ADCORRECTNormalized    []string       `datastore:"adcorrectnormalized"`
	EMAIL                  []string       `datastore:"email"`
	EMAILNormalized        []string       `datastore:"emailnormalized"`
	PHONE                  []string       `datastore:"phone"`
	PHONENormalized        []string       `datastore:"phonenormalized"`
	TRUSTEDID              []string       `datastore:"trustedid"`
	TRUSTEDIDNormalized    []string       `datastore:"trustedidnormalized"`
	CLIENTID               []string       `datastore:"clientid"`
	CLIENTIDNormalized     []string       `datastore:"clientidnormalized"`
	GENDER                 []string       `datastore:"gender"`
	GENDERNormalized       []string       `datastore:"gendernormalized"`
	AGE                    []string       `datastore:"age"`
	AGENormalized          []string       `datastore:"agenormalized"`
	DOB                    []string       `datastore:"dob"`
	DOBNormalized          []string       `datastore:"dobnormalized"`
	ORGANIZATION           []string       `datastore:"organization"`
	ORGANIZATIONNormalized []string       `datastore:"organizationnormalized"`
	TITLE                  []string       `datastore:"title"`
	TITLENormalized        []string       `datastore:"titlenormalized"`
	ROLE                   []string       `datastore:"role"`
	ROLENormalized         []string       `datastore:"rolenormalized"`
	STATUS                 []string       `datastore:"status"`
	STATUSNormalized       []string       `datastore:"statusnormalized"`
}

type PeopleGolden struct {
	ID           *datastore.Key `datastore:"__key__"`
	CreatedAt    time.Time      `datastore:"createdat"`
	SALUTATION   string         `datastore:"salutation"`
	NICKNAME     string         `datastore:"nickname"`
	FNAME        string         `datastore:"fname"`
	FINITIAL     string         `datastore:"finitial"`
	LNAME        string         `datastore:"lname"`
	MNAME        string         `datastore:"mname"`
	AD1          string         `datastore:"ad1"`
	AD1NO        string         `datastore:"ad1no"`
	AD2          string         `datastore:"ad2"`
	AD3          string         `datastore:"ad3"`
	CITY         string         `datastore:"city"`
	STATE        string         `datastore:"state"`
	ZIP          string         `datastore:"zip"`
	ZIP5         string         `datastore:"zip5"`
	COUNTRY      string         `datastore:"country"`
	MAILROUTE    string         `datastore:"mailroute"`
	ADTYPE       string         `datastore:"adtype"`
	ADBOOK       string         `datastore:"adbook"`
	ADPARSER     string         `datastore:"adparser"`
	ADCORRECT    string         `datastore:"adcorrect"`
	EMAIL        string         `datastore:"email"`
	PHONE        string         `datastore:"phone"`
	TRUSTEDID    string         `datastore:"trustedid"`
	CLIENTID     string         `datastore:"clientid"`
	GENDER       string         `datastore:"gender"`
	AGE          string         `datastore:"age"`
	DOB          string         `datastore:"dob"`
	ORGANIZATION string         `datastore:"organization"`
	TITLE        string         `datastore:"title"`
	ROLE         string         `datastore:"role"`
	STATUS       string         `datastore:"status"`
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
var DSKGolden = os.Getenv("DSKINDGOLDEN")

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
		var sets []PeopleSet
		var setIDs []string
		var golden []PeopleGolden

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

		if _, err := ds.GetAll(ctx, datastore.NewQuery(DSKSet).Namespace(OwnerNamespace).Filter("eventid =", input.RequestID), &sets); err != nil {
			log.Fatalf("Error querying sets: %v", err)
			return
		}
		log.Printf("sets retrieved: %v", sets)

		// get the set ids
		for _, s := range sets {
			setIDs = append(setIDs, s.ID.Name)
		}

		// if _, err := ds.GetAll(ctx, datastore.NewQuery(DSKSetMember).Namespace(OwnerNamespace).Filter("EventID =", input.RequestID), &setMembers); err != nil {
		// 	log.Fatalf("Error querying set members: %v", err)
		// 	return
		// }
		// log.Printf("set members retrieved: %v", setMembers)
		// for _, v := range setMembers {
		// 	if !Contains(setIDs, v.SetID) {
		// 		setIDs = append(setIDs, v.SetID)
		// 	}
		// }
		log.Printf("set id retrieved: %v", setIDs)

		var goldenKeys []*datastore.Key
		for _, s := range setIDs {
			dsGoldenKey := datastore.NameKey(DSKGolden, s, nil)
			dsGoldenKey.Namespace = OwnerNamespace
			goldenKeys = append(goldenKeys, dsGoldenKey)
			golden = append(golden, PeopleGolden{})
		}
		if len(goldenKeys) > 0 {
			if err := ds.GetMulti(ctx, goldenKeys, golden); err != nil && err != datastore.ErrNoSuchEntity {
				log.Fatalf("Error fetching fibers ns %v kind %v, keys %v: %v,", OwnerNamespace, DSKGolden, goldenKeys, err)
			}
		}

		PeopleMatchKeyNames := structs.Names(&PeopleMatchKeys{})

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

			for _, m := range PeopleMatchKeyNames {
				mk := GetMatchKeyFieldFromFiberByName(&f, m)
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

		for _, g := range golden {
			for _, m := range PeopleMatchKeyNames {
				mkValue := GetMatchKeyFieldFromFGoldenByName(&g, m)
				if m == "COUNTRY" {
					country := strings.ToUpper(mkValue)
					if country != "" && country != "US" && country != "USA" && country != "UNITED STATES" && country != "UNITED STATES OF AMERICA" {
						InternationalCount++
					}
				} else if m == "TITLE" {
					if IsInt(mkValue) {
						class, err := strconv.Atoi(mkValue)
						if err == nil {
							if class == CurrentYear+4 {
								FreshmenCount++
							} else if class >= CurrentYear && class < CurrentYear+4 {
								UpperclassmenCount++
							}
						}
					}

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
			Person:        len(sets),
			Dupe:          len(fibers) - len(sets),
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

func GetMatchKeyFieldFromFGoldenByName(v *PeopleGolden, field string) string {
	r := reflect.ValueOf(v)
	f := reflect.Indirect(r).FieldByName(field)
	return f.Interface().(string)
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
