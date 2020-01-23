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

type FileReport struct {
	RequestID   string
	RowCount    int
	ColumnCount int
	Columns     []ColumnStat
	ProcessedOn time.Time
	PcocessTime string
	Fibers      FiberCount
}

type DetailReport struct {
	Summary     DetailSummary
	GridRecords [][]interface{}
	GridFibers  [][]interface{}
}

type DetailSummary struct {
	EventType   string
	EventID     string
	Owner       string
	Source      string
	Attributes  map[string]string
	FileURL     string
	RowCount    int
	ColumnCount int
}

type DetailRecord struct {
	RecordID  string
	RowNumber int
	TimeStamp time.Time
	IsPeople  bool
	Record    []interface{}
	Fibers    [][]interface{}
}

type OwnerReport struct {
	FileCount int
	Requests  []Event
	Columns   []ColumnStat
	Counts    FiberCount
}

type ColumnStat struct {
	Name     string
	Min      string
	Max      string
	Sparsity float32
	Mapped   []string
}

type FiberCount struct {
	Person    int
	Dupe      int
	Invalid   int
	Throwaway int
	PurgePre  int
	Purge360  int
	Default   int
	MAR       int
	MPR       int
	NDFS      int
	NDFP      int
	NDUS      int
	NDUP      int
	NIFS      int
	NIFP      int
	NIUS      int
	NIUP      int
	EDFS      int
	EDFP      int
	EDUS      int
	EDUP      int
	EIFS      int
	EIFP      int
	EIUS      int
	EIUP      int
}
type SetCount struct {
	HouseHold int
}

type Record struct {
	EventType     string    `datastore:"Type"`
	EventID       string    `datastore:"EventID"`
	RecordID      string    `datastore:"RecordID"`
	RowNumber     int       `datastore:"RowNo"`
	Fields        []KVP     `datastore:"Fields,noindex"`
	TimeStamp     time.Time `datastore:"Created"`
	IsPeople      bool      `datastore:"IsPeople"`
	IsProduct     bool      `datastore:"IsProduct"`
	IsCampaign    bool      `datastore:"IsCampaign"`
	IsOrder       bool      `datastore:"IsOrder"`
	IsConsignment bool      `datastore:"IsConsignment"`
	IsOrderDetail bool      `datastore:"IsOrderDetail"`
	IsEvent       bool      `datastore:"IsEvent"`
	MLError       bool      `datastore:"MLError"`
}

type Fiber struct {
	ID           *datastore.Key   `datastore:"__key__"`
	CreatedAt    time.Time        `datastore:"createdat"`
	OwnerID      string           `datastore:"ownerid"`
	Source       string           `datastore:"source"`
	EventID      string           `datastore:"eventid"`
	EventType    string           `datastore:"eventtype"`
	RecordID     string           `datastore:"recordid"`
	FiberType    string           `datastore:"fibertype"`
	Disposition  string           `datastore:"disposition"`
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
	ZIPTYPE      MatchKeyField    `datastore:"ziptype"`
	RECORDTYPE   MatchKeyField    `datastore:"recordtype"`
	ADBOOK       MatchKeyField    `datastore:"adbook"`
	ADPARSER     MatchKeyField    `datastore:"adparser"`
	ADCORRECT    MatchKeyField    `datastore:"adcorrect"`
	ADVALID      MatchKeyField    `datastore:"advalid"`
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
	PermE        MatchKeyField    `datastore:"perme"`
	PermM        MatchKeyField    `datastore:"permm"`
	PermS        MatchKeyField    `datastore:"perms"`
	Passthrough  []Passthrough360 `datastore:"passthrough"`
}

type Signature struct {
	OwnerID   string
	Source    string
	EventID   string
	EventType string
	RecordID  string
	FiberType string
}

type Passthrough360 struct {
	Name  string
	Value string
}

type PeopleMatchKeys struct {
	SALUTATION   MatchKeyField `json:"salutation"`
	NICKNAME     MatchKeyField `json:"nickname"`
	FNAME        MatchKeyField `json:"fname"`
	FINITIAL     MatchKeyField `json:"finitial"`
	LNAME        MatchKeyField `json:"lname"`
	MNAME        MatchKeyField `json:"mname"`
	AD1          MatchKeyField `json:"ad1"`
	AD1NO        MatchKeyField `json:"ad1no"`
	AD2          MatchKeyField `json:"ad2"`
	AD3          MatchKeyField `json:"ad3"`
	CITY         MatchKeyField `json:"city"`
	STATE        MatchKeyField `json:"state"`
	ZIP          MatchKeyField `json:"zip"`
	ZIP5         MatchKeyField `json:"zip5"`
	COUNTRY      MatchKeyField `json:"country"`
	MAILROUTE    MatchKeyField `json:"mailroute"`
	ADTYPE       MatchKeyField `json:"adtype"`
	ADBOOK       MatchKeyField `json:"adbook"`
	ADPARSER     MatchKeyField `json:"adparser"`
	ADCORRECT    MatchKeyField `json:"adcorrect"`
	ADVALID      MatchKeyField `json:"advalid"`
	ZIPTYPE      MatchKeyField `json:"ziptype"`
	RECORDTYPE   MatchKeyField `json:"recordtype"`
	EMAIL        MatchKeyField `json:"email"`
	PHONE        MatchKeyField `json:"phone"`
	TRUSTEDID    MatchKeyField `json:"trustedId"`
	CLIENTID     MatchKeyField `json:"clientId"`
	GENDER       MatchKeyField `json:"gender"`
	AGE          MatchKeyField `json:"age"`
	DOB          MatchKeyField `json:"dob"`
	ORGANIZATION MatchKeyField `json:"organization"`
	TITLE        MatchKeyField `json:"title"`
	ROLE         MatchKeyField `json:"role"`
	STATUS       MatchKeyField `json:"status"`
	PermE        MatchKeyField `json:"perme"`
	PermM        MatchKeyField `json:"permm"`
	PermS        MatchKeyField `json:"perms"`
}

type HouseHoldMatchKeys struct {
	LNAME        MatchKeyField `json:"lname"`
	CITY         MatchKeyField `json:"city"`
	STATE        MatchKeyField `json:"state"`
	ZIP          MatchKeyField `json:"zip"`
	ZIP5         MatchKeyField `json:"zip5"`
	COUNTRY      MatchKeyField `json:"country"`
	AD1          MatchKeyField `json:"ad1"`
	AD1NO        MatchKeyField `json:"ad1no"`
	AD2          MatchKeyField `json:"ad2"`
	ADTYPE       MatchKeyField `json:"adtype"`
	MAILROUTE    MatchKeyField `json:"mailroute"`
	ADBOOK       MatchKeyField `json:"adbook"`
	ADPARSER     MatchKeyField `json:"adparser"`
	ADCORRECT    MatchKeyField `json:"adcorrect"`
	ADVALID      MatchKeyField `json:"advalid"`
	ZIPTYPE      MatchKeyField `json:"ziptype"`
	RECORDTYPE   MatchKeyField `json:"recordtype"`
	PermM        MatchKeyField `json:"permm"`
	ORGANIZATION MatchKeyField `json:"organization"`
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
	FiberType              []string       `datastore:"fibertype"`
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
	AD4                    []string       `datastore:"ad4"`
	AD4Normalized          []string       `datastore:"ad4normalized"`
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
	ZIPTYPE                []string       `datastore:"ziptype"`
	ZIPTYPENormalized      []string       `datastore:"ziptypenormalized"`
	RECORDTYPE             []string       `datastore:"recordtype"`
	RECORDTYPENormalized   []string       `datastore:"recordtypenormalized"`
	ADBOOK                 []string       `datastore:"adbook"`
	ADBOOKNormalized       []string       `datastore:"adbooknormalized"`
	ADPARSER               []string       `datastore:"adparser"`
	ADPARSERNormalized     []string       `datastore:"adparsernormalized"`
	ADCORRECT              []string       `datastore:"adcorrect"`
	ADCORRECTNormalized    []string       `datastore:"adcorrectnormalized"`
	ADVALID                []string       `datastore:"advalid"`
	ADVALIDNormalized      []string       `datastore:"advalidnormalized"`
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
	PermE                  []string       `json:"perme"`
	PermENormalized        []string       `json:"permenormalized"`
	PermM                  []string       `json:"permm"`
	PermMNormalized        []string       `json:"permmnormalized"`
	PermS                  []string       `json:"perms"`
	PermSNormalized        []string       `json:"permsnormalized"`
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
	ZIPTYPE      string         `datastore:"ziptype"`
	RECORDTYPE   string         `datastore:"recordtype"`
	ADBOOK       string         `datastore:"adbook"`
	ADPARSER     string         `datastore:"adparser"`
	ADCORRECT    string         `datastore:"adcorrect"`
	ADVALID      string         `datastore:"advalid"`
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
	PermE        string         `datastore:"perme"`
	PermM        string         `datastore:"permm"`
	PermS        string         `datastore:"perms"`
}

// ProjectID is the env var of project id
var ProjectID = os.Getenv("PROJECTID")
var DSProjectID = os.Getenv("DSPROJECTID")

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

	if !strings.EqualFold(input.ReportType, "file") && !strings.EqualFold(input.ReportType, "owner") && !strings.EqualFold(input.ReportType, "detail") && !strings.EqualFold(input.ReportType, "setfiber") && !strings.EqualFold(input.ReportType, "setrecord") {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "{\"success\": false, \"message\": \"reportType must be one of : file, owner, detail, setfiber or setrecord\"}")
		return
	}

	if !strings.EqualFold(input.ReportType, "file") && len(input.RequestID) < 36 {
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
	if _, err := fs.Put(ctx, eventKey, event); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		log.Fatalf("Error logging event: %v", err)
		fmt.Fprint(w, "{\"success\": false, \"message\": \"Internal error occurred, -3\"}")
		return
	}

	var output interface{}
	columns := make(map[string]ColumnStat)
	columnMaps := make(map[string][]string)
	PeopleMatchKeyNames := structs.Names(&PeopleMatchKeys{})
	if strings.EqualFold(input.ReportType, "file") {
		report := FileReport{
			RequestID: input.RequestID,
		}
		var records []Record
		var fibers []Fiber
		// var sets []PeopleSet
		// var setIDs []string
		// var golden []PeopleGolden

		if _, err := fs.GetAll(ctx, datastore.NewQuery(DSKRecord).Namespace(OwnerNamespace).Filter("EventID =", input.RequestID), &records); err != nil {
			log.Fatalf("Error querying records: %v", err)
			return
		}
		log.Printf("records retrieved: %v", len(records))

		if _, err := fs.GetAll(ctx, datastore.NewQuery(DSKFiber).Namespace(OwnerNamespace).Filter("eventid =", input.RequestID), &fibers); err != nil {
			log.Fatalf("Error querying fibers: %v", err)
			return
		}
		log.Printf("fibers retrieved: %v", len(fibers))

		// if _, err := fs.GetAll(ctx, datastore.NewQuery(DSKSet).Namespace(OwnerNamespace).Filter("eventid =", input.RequestID), &sets); err != nil {
		// 	log.Fatalf("Error querying sets: %v", err)
		// 	return
		// }
		// log.Printf("sets retrieved: %v", len(sets))

		// // get the set ids
		// for _, s := range sets {
		// 	setIDs = append(setIDs, s.ID.Name)
		// }

		// if _, err := fs.GetAll(ctx, datastore.NewQuery(DSKSetMember).Namespace(OwnerNamespace).Filter("EventID =", input.RequestID), &setMembers); err != nil {
		// 	log.Fatalf("Error querying set members: %v", err)
		// 	return
		// }
		// log.Printf("set members retrieved: %v", setMembers)
		// for _, v := range setMembers {
		// 	if !Contains(setIDs, v.SetID) {
		// 		setIDs = append(setIDs, v.SetID)
		// 	}
		// }
		// log.Printf("set id retrieved: %v", setIDs)

		// var goldenKeys []*datastore.Key
		// for _, s := range setIDs {
		// 	dsGoldenKey := datastore.NameKey(DSKGolden, s, nil)
		// 	dsGoldenKey.Namespace = OwnerNamespace
		// 	goldenKeys = append(goldenKeys, dsGoldenKey)
		// 	golden = append(golden, PeopleGolden{})
		// }
		// if len(goldenKeys) > 0 { // pull by batch of 1000
		// 	l := len(goldenKeys) / 1000
		// 	if len(goldenKeys)%1000 > 0 {
		// 		l++
		// 	}
		// 	for r := 0; r < l; r++ {
		// 		s := r * 1000
		// 		e := s + 1000

		// 		if e > len(goldenKeys) {
		// 			e = len(goldenKeys)
		// 		}

		// 		gk := goldenKeys[s:e]
		// 		gd := golden[s:e]

		// 		if err := fs.GetMulti(ctx, gk, gd); err != nil && err != datastore.ErrNoSuchEntity {
		// 			log.Printf("Error fetching golden records ns %v kind %v, key count %v: %v,", OwnerNamespace, DSKGolden, len(goldenKeys), err)
		// 		}

		// 	}
		// }

		NDFS := 0 // new domestic freshmen student
		NDFP := 0 // new domestic freshmen parent
		NDUS := 0 // new domestic upperclassman student
		NDUP := 0 // new domestic upperclassman parent
		NIFS := 0 // new international freshmen student
		NIFP := 0 // new international freshmen parent
		NIUS := 0 // new international upperclassman student
		NIUP := 0 // new international upperclassman parent

		EDFS := 0 // existing domestic freshmen student
		EDFP := 0 // existing domestic freshmen parent
		EDUS := 0 // existing domestic upperclassman student
		EDUP := 0 // existing domestic upperclassman parent
		EIFS := 0 // existing international freshmen student
		EIFP := 0 // existing international freshmen parent
		EIUS := 0 // existing international upperclassman student
		EIUP := 0 // existing international upperclassman parent

		recordIDs := []string{}
		recordUniques := []string{}
		newIDs := []string{}

		DUPE := 0
		CurrentYear := time.Now().Year()

		PURGE1 := 0
		PURGE2 := 0
		INVALID := 0
		DEFAULT := 0
		MAR := 0
		MPR := 0

		for _, r := range records {
			if !r.IsPeople {
				PURGE1++
			}
			if Contains(recordUniques, r.RecordID) {
				continue
			}
			recordUniques = append(recordUniques, r.RecordID)
		}

		for _, f := range fibers {
			if f.FiberType == "mar" || f.FiberType == "default" {
				if f.Disposition == "new" {
					newIDs = append(newIDs, f.RecordID)
				}
			}
			switch f.FiberType {
			case "mpr":
				MPR++
			case "mar":
				MAR++
			case "default":
				DEFAULT++
			}
		}

		for _, f := range fibers {
			for _, m := range PeopleMatchKeyNames {
				mk := GetMatchKeyFieldFromFiberByName(&f, m)
				columnTarget := []string{m}
				source := strings.ToUpper(mk.Source)
				if len(mk.Source) > 0 {
					if val, ok := columnMaps[source]; ok {
						columnTarget = val
						if !Contains(columnTarget, m) {
							columnTarget = append(columnTarget, m)
						}
					}
					columnMaps[source] = columnTarget
				}
			}

			// recordID := Left(f.RecordID, 36)
			// if !Contains(recordIDs, recordID) {
			// 	recordIDs = append(recordIDs, recordID)
			// }
			isInternational := false
			isDomestic := false
			isParent := false
			isUpperClassman := false
			isFreshmen := false
			isNew := false

			if f.FiberType == "mar" { // skip mar
				continue
			} else if f.FiberType == "default" { //same record id processed twice?
				if Contains(recordIDs, f.RecordID) {
					continue
				}
				recordIDs = append(recordIDs, f.RecordID)
			} else if f.FiberType == "mpr" {
				isParent = true
			}

			if f.Disposition == "purge" {
				if f.FiberType == "default" {
					PURGE2++
				}

			} else if f.Disposition == "dupe" {
				if f.FiberType == "default" {
					DUPE++
				}
			} else {
				if f.Disposition == "new" {
					isNew = true
				} else if Contains(newIDs, f.RecordID) {
					isNew = true
				}

				country := strings.ToUpper(GetMatchKeyFieldFromFiberByName(&f, "COUNTRY").Value)
				//if country != "" && country != "US" && country != "USA" && country != "UNITED STATES" && country != "UNITED STATES OF AMERICA" {
				if country != "US" {
					isInternational = true
				} else {
					isDomestic = true
				}
				class, err := strconv.Atoi(GetMatchKeyFieldFromFiberByName(&f, "TITLE").Value)
				if err == nil {
					if class == CurrentYear+4 || (class == CurrentYear+3 && time.Now().Month() < 6) {
						isFreshmen = true
					} else if (class > CurrentYear-1 || (class == CurrentYear-1 && time.Now().Month() >= 6)) && (class < CurrentYear+3 || (class == CurrentYear+3 && time.Now().Month() >= 6)) {
						isUpperClassman = true
					}
				}
				// role := GetMatchKeyFieldFromFiberByName(&f, "ROLE").Value
				// if role == "Parent" {
				// 	isParent = true
				// }

				// all international addresses would be invalid... guess we just want US invalid...
				if f.ADVALID.Value == "FALSE" {
					if f.FiberType == "default" {
						INVALID++
					}
					continue
				}

				if isNew && isDomestic && isFreshmen && !isParent {
					NDFS++
				} else if isNew && isDomestic && isFreshmen && isParent {
					NDFP++
				} else if isNew && isDomestic && isUpperClassman && !isParent {
					NDUS++
				} else if isNew && isDomestic && isUpperClassman && isParent {
					NDUP++
				} else if isNew && isInternational && isFreshmen && !isParent {
					NIFS++
				} else if isNew && isInternational && isFreshmen && isParent {
					NIFP++
				} else if isNew && isInternational && isUpperClassman && !isParent {
					NIUS++
				} else if isNew && isInternational && isUpperClassman && isParent {
					NIUP++
				} else if !isNew && isDomestic && isFreshmen && !isParent {
					EDFS++
				} else if !isNew && isDomestic && isFreshmen && isParent {
					EDFP++
				} else if !isNew && isDomestic && isUpperClassman && !isParent {
					EDUS++
				} else if !isNew && isDomestic && isUpperClassman && isParent {
					EDUP++
				} else if !isNew && isInternational && isFreshmen && !isParent {
					EIFS++
				} else if !isNew && isInternational && isFreshmen && isParent {
					EIFP++
				} else if !isNew && isInternational && isUpperClassman && !isParent {
					EIUS++
				} else if !isNew && isInternational && isUpperClassman && isParent {
					EIUP++
				}
			}

		}

		// for _, g := range golden {

		// 	for _, m := range PeopleMatchKeyNames {
		// 		mkValue := GetMatchKeyFieldFromFGoldenByName(&g, m)
		// 		if m == "COUNTRY" {
		// 			country := strings.ToUpper(mkValue)
		// 			if country != "" && country != "US" && country != "USA" && country != "UNITED STATES" && country != "UNITED STATES OF AMERICA" {
		// 				isInternational = true
		// 			}
		// 		} else if m == "TITLE" {
		// 			if IsInt(mkValue) {
		// 				class, err := strconv.Atoi(mkValue)
		// 				if err == nil {
		// 					if class == CurrentYear+4 {
		// 						isFreshmen = true
		// 					} else if class >= CurrentYear && class < CurrentYear+4 {
		// 						isUpperClassman = true
		// 					}
		// 				}
		// 			}
		// 		} else if m == "ROLE" {
		// 			if mkValue == "Parent" {
		// 				isParent = true
		// 			}
		// 		}
		// 	}
		// 	if isNew && !isInternational && isFreshmen && !isParent {
		// 		NDFS++
		// 	} else if isNew && !isInternational && isFreshmen && isParent {
		// 		NDFP++
		// 	} else if isNew && !isInternational && isUpperClassman && !isParent {
		// 		NDUS++
		// 	} else if isNew && !isInternational && isUpperClassman && isParent {
		// 		NDUP++
		// 	} else if isNew && isInternational && isFreshmen && !isParent {
		// 		NIFS++
		// 	} else if isNew && isInternational && isFreshmen && isParent {
		// 		NIFP++
		// 	} else if isNew && isInternational && isUpperClassman && !isParent {
		// 		NIUS++
		// 	} else if isNew && isInternational && isUpperClassman && isParent {
		// 		NIUP++
		// 	} else if !isNew && !isInternational && isFreshmen && !isParent {
		// 		EDFS++
		// 	} else if !isNew && !isInternational && isFreshmen && isParent {
		// 		EDFP++
		// 	} else if !isNew && !isInternational && isUpperClassman && !isParent {
		// 		EDUS++
		// 	} else if !isNew && !isInternational && isUpperClassman && isParent {
		// 		EDUP++
		// 	} else if !isNew && isInternational && isFreshmen && !isParent {
		// 		EIFS++
		// 	} else if !isNew && isInternational && isFreshmen && isParent {
		// 		EIFP++
		// 	} else if !isNew && isInternational && isUpperClassman && !isParent {
		// 		EIUS++
		// 	} else if !isNew && isInternational && isUpperClassman && isParent {
		// 		EIUP++
		// 	}
		// }
		log.Printf("column maps: %v", columnMaps)

		report.RowCount = len(recordUniques)
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
			columnName := strings.ToUpper(v.Name)
			v.Mapped = columnMaps[columnName]
			report.Columns = append(report.Columns, v)
		}

		sort.Slice(report.Columns, func(i, j int) bool {
			return strings.Compare(report.Columns[i].Name, report.Columns[j].Name) > 0
		})

		report.PcocessTime = fmt.Sprintf("%v s", maxTime.Sub(minTime).Seconds())
		report.ProcessedOn = minTime

		report.Fibers = FiberCount{
			Person:    len(recordIDs) + MPR,
			Dupe:      DUPE,
			PurgePre:  PURGE1,
			Purge360:  PURGE2,
			Throwaway: PURGE1 + PURGE2, //len(records) - len(recordIDs), // unique record id, take first 36 characters of record id, to avoid counting MPR records
			Default:   len(recordIDs),
			Invalid:   INVALID,
			MAR:       MAR,
			MPR:       MPR,
			EDFS:      EDFS,
			EDFP:      EDFP,
			EDUS:      EDUS,
			EDUP:      EDUP,
			EIFS:      EIFS,
			EIFP:      EIFP,
			EIUS:      EIUS,
			EIUP:      EIUP,
			NDFS:      NDFS,
			NDFP:      NDFP,
			NDUS:      NDUS,
			NDUP:      NDUP,
			NIFS:      NIFS,
			NIFP:      NIFP,
			NIUS:      NIUS,
			NIUP:      NIUP,
		}
		output = report
	} else if strings.EqualFold(input.ReportType, "detail") {
		report := DetailReport{}
		var records []Record
		var fibers []Fiber
		var requests []Event
		var request Event

		var summary DetailSummary

		query := datastore.NewQuery("Event").Namespace(NameSpace).Filter("EventID =", input.RequestID).Limit(1)

		if _, err := fs.GetAll(ctx, query, &requests); err != nil {
			log.Fatalf("Error querying event: %v", err)
			return
		} else if len(requests) > 0 {
			request = requests[0]
			summary.EventType = request.EventType
			summary.EventID = request.EventID
			summary.Owner = request.Owner
			summary.Source = request.Source
			summary.FileURL = request.Detail
			summary.Attributes = ToMap(request.Attributes)
		}

		log.Printf("Found %v matching events", len(requests))

		if _, err := fs.GetAll(ctx, datastore.NewQuery(DSKRecord).Namespace(OwnerNamespace).Filter("EventID =", input.RequestID), &records); err != nil {
			log.Fatalf("Error querying records: %v", err)
			return
		}
		log.Printf("records retrieved: %v", len(records))
		// sort records
		sort.Slice(records, func(i, j int) bool {
			return records[i].RowNumber < records[j].RowNumber
		})

		if _, err := fs.GetAll(ctx, datastore.NewQuery(DSKFiber).Namespace(OwnerNamespace).Filter("eventid =", input.RequestID), &fibers); err != nil {
			log.Fatalf("Error querying fibers: %v", err)
			return
		}
		log.Printf("fibers retrieved: %v", len(fibers))

		// organize fibers by record
		fibermap := make(map[string][]Fiber)
		for _, f := range fibers {
			recordID := Left(f.RecordID, 36)
			if _, ok := fibermap[recordID]; ok {

			} else {
				fibermap[recordID] = []Fiber{}
			}
			fibermap[recordID] = append(fibermap[recordID], f)
		}

		report.GridRecords = append(report.GridRecords, []interface{}{"RecordID", "RowNumber", "TimeStamp", "Disposition"})

		diagnostics := []string{"IsPeople", "MLError", "FiberCount", "PersonFiberCount", "MARFiberCount", "MPRFiberCount"}
		report.GridFibers = append(report.GridFibers, []interface{}{"RecordID", "RowNumber", "FiberNumber", "FiberID", "TimeStamp", "Type", "Disposition", "IsValid"})

		for _, k := range PeopleMatchKeyNames {
			report.GridFibers[0] = append(report.GridFibers[0], k)
			report.GridFibers[0] = append(report.GridFibers[0], "Source")
		}

		// var gridFiber [][]interface{}
		summary.RowCount = len(records)
		columns := make(map[string]ColumnStat)

		for i, r := range records {
			var rowRecord []interface{}
			rowRecord = append(rowRecord, r.RecordID)
			rowRecord = append(rowRecord, r.RowNumber)
			rowRecord = append(rowRecord, r.TimeStamp)

			fiberCount := 0
			marFiberCount := 0
			mprFiberCount := 0
			defaultFiberCount := 0

			columnMaps := make(map[string][]string)
			recordDisposition := "update"
			anyFiberIsNew := false
			allFibersAreDupe := true
			allFibersArePurged := true

			if _, ok := fibermap[r.RecordID]; ok {
				fiberCount = len(fibermap[r.RecordID])
				sort.Slice(fibermap[r.RecordID], func(i int, j int) bool {
					return fibermap[r.RecordID][i].CreatedAt.Before(fibermap[r.RecordID][j].CreatedAt)
				})
				for j, f := range fibermap[r.RecordID] {
					var rowFiber []interface{}

					rowFiber = append(rowFiber, r.RecordID)
					rowFiber = append(rowFiber, r.RowNumber)
					rowFiber = append(rowFiber, j+1)
					rowFiber = append(rowFiber, f.ID.Name)
					rowFiber = append(rowFiber, f.CreatedAt)
					rowFiber = append(rowFiber, f.FiberType)
					rowFiber = append(rowFiber, f.Disposition)
					rowFiber = append(rowFiber, "TRUE")

					for _, k := range PeopleMatchKeyNames {
						mk := GetMatchKeyFieldFromFiberByName(&f, k)
						rowFiber = append(rowFiber, mk.Value)
						rowFiber = append(rowFiber, mk.Source)
					}
					report.GridFibers = append(report.GridFibers, rowFiber)
					switch f.FiberType {
					case "mar":
						marFiberCount++
					case "mpr":
						mprFiberCount++
					case "default":
						defaultFiberCount++
					}

					if f.FiberType == "default" || f.FiberType == "mar" {
						if f.Disposition == "new" {
							anyFiberIsNew = true
						}
						if f.Disposition != "dupe" {
							allFibersAreDupe = false
						}
						if f.Disposition != "purge" {
							allFibersArePurged = false
						}

					}

					for _, m := range PeopleMatchKeyNames {
						mk := GetMatchKeyFieldFromFiberByName(&f, m)
						columnTarget := []string{m}
						source := mk.Source
						if len(mk.Source) > 0 {
							if val, ok := columnMaps[source]; ok {
								columnTarget = val
								if !Contains(columnTarget, m) {
									target := m
									if f.FiberType == "mar" || f.FiberType == "mpr" {
										target = f.FiberType + " " + m
									}
									columnTarget = append(columnTarget, target)

								}
							}
							columnMaps[source] = columnTarget
						}
					}
				}

			}
			if anyFiberIsNew {
				recordDisposition = "new"
			} else if allFibersAreDupe {
				recordDisposition = "dupe"
			} else if allFibersArePurged {
				recordDisposition = "purge"
			}
			rowRecord = append(rowRecord, recordDisposition)

			headers := []string{}
			if len(r.Fields) > 0 && i == 0 {
				for _, f := range r.Fields {
					headers = append(headers, f.Key)
				}
				sort.Strings(headers)
			}
			for _, h := range headers {
				report.GridRecords[0] = append(report.GridRecords[0], h)
				report.GridRecords[0] = append(report.GridRecords[0], "MappedTo")
			}
			values := make(map[string]string)
			mapped := make(map[string]string)

			for _, f := range r.Fields {
				values[f.Key] = f.Value
				if val, ok := columnMaps[f.Key]; ok {
					mapped[f.Key] = strings.Join(val, ", ")
				} else {
					mapped[f.Key] = ""
				}
			}
			for c, f := range report.GridRecords[0] {
				if c < 4 { // skip first 8 columns
					continue
				}
				if f.(string) == "MappedTo" {
					continue
				}
				if val, ok := values[f.(string)]; ok {
					rowRecord = append(rowRecord, val)
				} else {
					rowRecord = append(rowRecord, "")
				}

				// append mapped
				if val, ok := mapped[f.(string)]; ok {
					rowRecord = append(rowRecord, val)
				} else {
					rowRecord = append(rowRecord, "")
				}

			}

			rowRecord = append(rowRecord, r.IsPeople)
			rowRecord = append(rowRecord, r.MLError)

			rowRecord = append(rowRecord, fiberCount)
			rowRecord = append(rowRecord, defaultFiberCount)
			rowRecord = append(rowRecord, marFiberCount)
			rowRecord = append(rowRecord, mprFiberCount)

			report.GridRecords = append(report.GridRecords, rowRecord)
		}

		for _, d := range diagnostics {
			report.GridRecords[0] = append(report.GridRecords[0], d)
		}

		summary.ColumnCount = len(columns)
		report.Summary = summary

		output = report
	} else if strings.EqualFold(input.ReportType, "setfiber") {
		var sets []PeopleSet
		report := DetailReport{}
		report.GridRecords = append(report.GridRecords, []interface{}{"SetID", "FiberID"})
		if _, err := fs.GetAll(ctx, datastore.NewQuery(DSKSet).Namespace(OwnerNamespace).Filter("eventid =", input.RequestID), &sets); err != nil {
			log.Fatalf("Error querying sets: %v", err)
			return
		}
		log.Printf("sets retrieved: %v", len(sets))
		for _, r := range sets {
			for _, f := range r.Fibers {
				var rowRecord []interface{}
				rowRecord = append(rowRecord, r.ID.Name)
				rowRecord = append(rowRecord, f)
				report.GridRecords = append(report.GridRecords, rowRecord)
			}
		}
		output = report
	} else if strings.EqualFold(input.ReportType, "setrecord") {
		var sets []PeopleSet
		report := DetailReport{}
		report.GridRecords = append(report.GridRecords, []interface{}{"SetID", "RecordID"})
		if _, err := fs.GetAll(ctx, datastore.NewQuery(DSKSet).Namespace(OwnerNamespace).Filter("eventid =", input.RequestID), &sets); err != nil {
			log.Fatalf("Error querying sets: %v", err)
			return
		}
		log.Printf("sets retrieved: %v", len(sets))
		for _, r := range sets {
			for _, f := range r.RecordID {
				var rowRecord []interface{}
				rowRecord = append(rowRecord, r.ID.Name)
				rowRecord = append(rowRecord, f)
				report.GridRecords = append(report.GridRecords, rowRecord)
			}
		}
		output = report
	} else {
		report := OwnerReport{}

		output = report
	}
	outputJSON, err := json.Marshal(output)
	if err != nil {
		log.Fatalf("Error writing json %v", err)
	}
	fmt.Fprintf(w, string(outputJSON))
}

func ToMap(v []KVP) map[string]string {
	result := make(map[string]string)
	for _, kvp := range v {
		result[kvp.Key] = kvp.Value
	}
	return result
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
