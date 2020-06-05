// Package checkutil is a CF that performs purging on DS/BQ
package checkutil

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/datastore"
	"cloud.google.com/go/storage"
)

// ProjectID is the env var of project id
var pid = os.Getenv("PROJECTID")
var did = os.Getenv("DSPROJECTID")

// global vars
var ctx context.Context
var ds *datastore.Client
var fs *datastore.Client
var sb *storage.BucketHandle
var sbb *storage.BucketHandle
var sb2 *storage.BucketHandle

func init() {
	ctx = context.Background()
	ds, _ = datastore.NewClient(ctx, pid)
	fs, _ = datastore.NewClient(ctx, did)
	cs, _ := storage.NewClient(ctx)
	sb = cs.Bucket(os.Getenv("BUCKET"))
	sbb = cs.Bucket(os.Getenv("BADBUCKET"))
	sb2 = cs.Bucket("wm_missed_uploaded_prod")
	log.Printf("init completed")
}

// ProcessRequest Receives a http event request
func ProcessRequest(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Namespace string `json:"namespace"`
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

	w.WriteHeader(http.StatusOK)
	getSetsWithBlankEventId(input.Namespace)
	return
}

func getSetsWithBlankEventId(namespace string) {
	var sets []PeopleSetDS
	total := 0
	setQuery := datastore.NewQuery("people-set").Namespace(namespace)
	if _, err := fs.GetAll(ctx, setQuery, &sets); err != nil {
		log.Fatalf("Error querying sets: %v", err)
	}
	fiberIDs := []string{}
	mySets := []PeopleSetDS{}
	for _, set := range sets {
		total++
		if len(set.EventID) == 0 {
			for _, f := range set.Fibers {
				if !Contains(fiberIDs, f) {
					fiberIDs = append(fiberIDs, f)
				}
			}
			mySets = append(mySets, set)
		}
	}

	// now let's query fibers and get the event ids
	var fiberKeys []*datastore.Key
	var myFibers []PeopleFiberDS

	for _, id := range fiberIDs {
		dsFiberKey := datastore.NameKey("people-fiber", id, nil)
		dsFiberKey.Namespace = namespace
		fiberKeys = append(fiberKeys, dsFiberKey)
		myFibers = append(myFibers, PeopleFiberDS{})
	}
	batchSize := 1000

	if len(fiberKeys) > 0 {
		l := len(fiberKeys) / batchSize
		if len(fiberKeys)%batchSize > 0 {
			l++
		}
		for r := 0; r < l; r++ {
			s := r * 1000
			e := s + 1000

			if e > len(fiberKeys) {
				e = len(fiberKeys)
			}
			gk := fiberKeys[s:e]
			gd := myFibers[s:e]
			if err := fs.GetMulti(ctx, gk, gd); err != nil && err != datastore.ErrNoSuchEntity {
				log.Printf("Error fetching fibers records ns %v kind %v, key count %v: %v,", namespace, "people-fiber", len(fiberKeys), err)
			}
		}
	}

	// make a fiber map
	fiberIDMap := make(map[string]PeopleFiberDS)
	for _, f := range myFibers {
		fiberIDMap[f.ID.Name] = f
	}

	// now that we have the matching fibers for the sets that are missing event id, we can look up the event id from the fiber
	eventToSetIDMap := make(map[string][]string)
	for _, s := range mySets {
		log.Printf("checking set %v", s.ID.Name)
		for _, f := range s.Fibers {
			if fiber, ok := fiberIDMap[f]; ok {
				eventID := fiber.EventID
				if event, yes := eventToSetIDMap[eventID]; yes {
					if !Contains(eventToSetIDMap[eventID], s.ID.Name) {
						eventToSetIDMap[eventID] = append(event, s.ID.Name)
					}
				} else {
					eventToSetIDMap[eventID] = []string{s.ID.Name}
				}
			}
		}
	}

	// by now i should have a list of event to a []set, write these out
	header := []string{
		"School Code", "CRM", "Processor", "Sponsor", "Input Type", "Class Year", "Program", "Adcode", "Date Uploaded", "Order By Date", "List Type", "Salutation",
		"Student First Name", "Student Last Name", "Street Address 1", "Street Address 2", "City", "State", "Zipcode", "Country", "Student's Email_1", "Student's Email_2",
		"Parent_1's First Name", "Parent_1's Last Name", "Parent_1's Email", "Parent_2's First Name", "Parent_2's Last Name", "Parent_2's Email"}

	for eventID, setIDs := range eventToSetIDMap {
		//query the event
		var events []Event
		var event Event
		eventQuery := datastore.NewQuery("Event").Namespace("wemade-prod").Filter("EventID =", eventID).Limit(1)
		if _, err := fs.GetAll(ctx, eventQuery, &events); err != nil {
			log.Fatalf("Error querying event: %v", err)
		} else if len(events) > 0 {
			event = events[0]
			// get the golden

			var goldenKeys []*datastore.Key
			var goldens []PeopleGoldenDS
			for _, setKey := range setIDs {
				dsGoldenGetKey := datastore.NameKey("people-golden", setKey, nil)
				dsGoldenGetKey.Namespace = namespace
				goldenKeys = append(goldenKeys, dsGoldenGetKey)
				goldens = append(goldens, PeopleGoldenDS{})
			}
			if len(goldenKeys) > 0 {
				batchSize := 1000
				l := len(goldenKeys) / batchSize
				if len(goldenKeys)%batchSize > 0 {
					l++
				}
				for r := 0; r < l; r++ {
					s := r * 1000
					e := s + 1000
					if e > len(goldenKeys) {
						e = len(goldenKeys)
					}
					gk := goldenKeys[s:e]
					gd := goldens[s:e]
					if err := fs.GetMulti(ctx, gk, gd); err != nil && err != datastore.ErrNoSuchEntity {
						log.Printf("Error fetching golden records ns %v kind %v, key count %v: %v,", namespace, "people-golden", len(goldenKeys), err)
					}

				}
			}

			records := [][]string{header}
			for _, g := range goldens {
				//only students
				if g.ROLE == "Parent" {
					continue
				}

				//only students with address
				if len(g.AD1) == 0 {
					continue
				}

				row := []string{
					GetKVPValue(event.Passthrough, "schoolCode"),
					"",
					"",
					GetKVPValue(event.Passthrough, "schoolName"),
					GetKVPValue(event.Passthrough, "inputType"),
					schoolYearFormatter(GetKVPValue(event.Passthrough, "schoolYear"), GetKVPValue(event.Attributes, "classStanding")),
					GetKVPValue(event.Passthrough, "masterProgramCode"),
					GetKVPValue(event.Passthrough, "ADCODE"),
					event.Created.Format("01/02/2006"),
					GetKVPValue(event.Passthrough, "orderByDate"),
					listTypeFormatter(GetKVPValue(event.Passthrough, "listType")),
					GetKVPValue(event.Passthrough, "salutation"),
					g.FNAME,
					g.LNAME,
					g.AD1,
					g.AD2,
					g.CITY,
					g.STATE,
					g.ZIP,
					g.COUNTRY,
					strings.Split(g.EMAIL, "|")[0], // only write one email to CP
					"",
					"",
					"",
					"",
					"",
					"",
					"",
				}

				records = append(records, row)
			}

			// write the file
			var buf bytes.Buffer
			csv := csv.NewWriter(&buf)
			csv.WriteAll(records)
			csv.Flush()

			csvBytes := buf.Bytes()
			suppressFile := true

			file := sb.Object(GetKVPValue(event.Passthrough, "sponsorCode") + "." + GetKVPValue(event.Passthrough, "masterProgramCode") + "." + GetKVPValue(event.Passthrough, "schoolYear") + "." + eventID + "." + strconv.Itoa(len(records)-1) + ".csv")
			if suppressFile {
				file = sb2.Object(GetKVPValue(event.Passthrough, "sponsorCode") + "." + GetKVPValue(event.Passthrough, "masterProgramCode") + "." + GetKVPValue(event.Passthrough, "schoolYear") + "." + eventID + "." + strconv.Itoa(len(records)-1) + ".csv")
			}
			writer := file.NewWriter(ctx)
			if _, err := io.Copy(writer, bytes.NewReader(csvBytes)); err != nil {
				log.Printf("File cannot be copied to bucket %v", err)
			}
			if err := writer.Close(); err != nil {
				log.Printf("Failed to close bucket write stream %v", err)
			}
		}
	}

}

type PeopleSetDS struct {
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
	Search                 []string       `datastore:"search"`
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
	PermE                  []string       `datastore:"perme"`
	PermENormalized        []string       `datastore:"permenormalized"`
	PermM                  []string       `datastore:"permm"`
	PermMNormalized        []string       `datastore:"permmnormalized"`
	PermS                  []string       `datastore:"perms"`
	PermSNormalized        []string       `datastore:"permsnormalized"`
}

type PeopleGoldenDS struct {
	ID           *datastore.Key `datastore:"__key__"`
	CreatedAt    time.Time      `datastore:"createdat"`
	Search       []string       `datastore:"search"`
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

type PeopleFiberDS struct {
	ID           *datastore.Key   `datastore:"__key__"`
	CreatedAt    time.Time        `datastore:"createdat"`
	OwnerID      string           `datastore:"ownerid"`
	Source       string           `datastore:"source"`
	EventID      string           `datastore:"eventid"`
	EventType    string           `datastore:"eventtype"`
	RecordID     string           `datastore:"recordid"`
	FiberType    string           `datastore:"fibertype"`
	Disposition  string           `datastore:"disposition"`
	Search       []string         `datastore:"search"`
	Passthrough  []Passthrough360 `datastore:"passthrough"`
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
}

type Passthrough360 struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type MatchKeyField struct {
	Value  string `json:"value"`
	Source string `json:"source"`
	Type   string `json:"type"`
}

func Contains(slice []string, item string) bool {
	for _, v := range slice {
		if strings.EqualFold(v, item) {
			return true
		}
	}
	return false
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
	EventData   []KVP
	Detail      string
	RowLimit    int
	Counters    []KIP
}

type KVP struct {
	Key   string `json:"k" datastore:"k"`
	Value string `json:"v" datastore:"v"`
}

type KIP struct {
	Key   string `json:"k" datastore:"k"`
	Value int    `json:"v" datastore:"v"`
}

func GetKVPValue(v []KVP, key string) string {
	for _, kvp := range v {
		if kvp.Key == key {
			return kvp.Value
		}
	}
	return ""
}

func schoolYearFormatter(schoolYear, classStanding string) string {
	// LogDev(fmt.Sprintf("schoolYear: %v classStanding: %v", schoolYear, classStanding))
	if classStanding == "Freshman" {
		return "FY" + schoolYear
	}
	return schoolYear
}

func listTypeFormatter(listType string) string {
	// LogDev(fmt.Sprintf("listType: %v", listType))
	if len(listType) > 0 {
		return listType[0:1]
	}
	return listType
}
