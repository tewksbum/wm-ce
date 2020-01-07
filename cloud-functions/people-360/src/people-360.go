package people360

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode"

	"cloud.google.com/go/datastore"
	"cloud.google.com/go/pubsub"

	"github.com/fatih/structs"
	"github.com/gomodule/redigo/redis"
	"github.com/google/uuid"
)

type PubSubMessage struct {
	Data []byte `json:"data"`
}

type Signature struct {
	OwnerID   string `json:"ownerId"`
	Source    string `json:"source"`
	EventID   string `json:"eventId"`
	EventType string `json:"eventType"`
	FiberType string `json:"fiberType"`
	RecordID  string `json:"recordId"`
}

type EventData struct {
	Signature   Signature              `json:"signature"`
	Passthrough map[string]string      `json:"passthrough"`
	Attributes  map[string]string      `json:"attributes"`
	EventData   map[string]interface{} `json:"eventData"`
}

type PeopleInput struct {
	Signature   Signature         `json:"signature"`
	Passthrough map[string]string `json:"passthrough"`
	MatchKeys   PeopleOutput      `json:"matchkeys`
	// MatchKeys   map[PeopleOutput]PeopleOutput      `json:"matchkeys`
}

type PeopleFiber struct {
	Signature Signature `json:"signature"`
	// Passthrough map[string]string `json:"passthrough"`
	Passthrough []Passthrough360 `json:"passthrough"`
	MatchKeys   PeopleOutput     `json:"matchkeys"`
	ID          string           `json:"fiberId"`
	CreatedAt   time.Time        `json:"createdAt"`
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
	PermE        MatchKeyField    `datastore:"PermE"`
	PermM        MatchKeyField    `datastore:"PermM"`
	PermS        MatchKeyField    `datastore:"PermS"`
	Passthrough  []Passthrough360 `datastore:"passthrough"`
}

type MatchKeyField struct {
	Value  string `json:"value"`
	Source string `json:"source"`
	Type   string `json:"type"`
}

type PeopleOutput struct {
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

type Signature360 struct {
	OwnerID   string `json:"ownerId"`
	Source    string `json:"source"`
	EventID   string `json:"eventId"`
	EventType string `json:"eventType"`
}

type MatchKey360 struct {
	Key    string   `json:"key"`
	Type   string   `json:"type"`
	Value  string   `json:"value"`
	Values []string `json:"values"`
}

type Passthrough360 struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type People360Output struct {
	ID           string           `json:"id"`
	Signature    Signature360     `json:"signature"`
	Signatures   []Signature      `json:"signatures"`
	CreatedAt    time.Time        `json:"createdAt"`
	Fibers       []string         `json:"fibers"`
	Passthroughs []Passthrough360 `json:"passthroughs"`
	MatchKeys    []MatchKey360    `json:"matchKeys"`
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

type PeopleGoldenDS struct {
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

var ProjectID = os.Getenv("PROJECTID")
var SetTableName = os.Getenv("SETTABLE")
var FiberTableName = os.Getenv("FIBERTABLE")
var ESUrl = os.Getenv("ELASTICURL")
var ESUid = os.Getenv("ELASTICUSER")
var ESPwd = os.Getenv("ELASTICPWD")
var ESIndex = os.Getenv("ELASTICINDEX")
var Env = os.Getenv("ENVIRONMENT")
var dev = Env == "dev"
var DSKindSet = os.Getenv("DSKINDSET")
var DSKindGolden = os.Getenv("DSKINDGOLDEN")
var DSKindFiber = os.Getenv("DSKINDFIBER")

var reAlphaNumeric = regexp.MustCompile("[^a-zA-Z0-9]+")

var redisTransientExpiration = 3600 * 24

var ps *pubsub.Client
var topic *pubsub.Topic
var topic2 *pubsub.Topic
var status *pubsub.Topic
var ds *datastore.Client
var msp *redis.Pool

// var setSchema bigquery.Schema

func init() {
	ctx := context.Background()
	ps, _ = pubsub.NewClient(ctx, ProjectID)
	ds, _ = datastore.NewClient(ctx, ProjectID)
	topic = ps.Topic(os.Getenv("PSOUTPUT"))
	topic2 = ps.Topic(os.Getenv("PSOUTPUT2"))
	status = ps.Topic(os.Getenv("PSSTATUS"))
	msp = &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial:        func() (redis.Conn, error) { return redis.Dial("tcp", os.Getenv("MEMSTORE")) },
	}
	log.Printf("init completed, pubsub topic name: %v", topic)
}

func People360(ctx context.Context, m PubSubMessage) error {
	var input PeopleInput
	if err := json.Unmarshal(m.Data, &input); err != nil {
		log.Fatalf("Unable to unmarshal message %v with error %v", string(m.Data), err)
	}

	// assign first initial and zip5
	if len(input.MatchKeys.FNAME.Value) > 0 {
		input.MatchKeys.FINITIAL = MatchKeyField{
			Value:  input.MatchKeys.FNAME.Value[0:1],
			Source: input.MatchKeys.FNAME.Source,
		}
	}
	if len(input.MatchKeys.ZIP.Value) > 5 {
		input.MatchKeys.ZIP5 = MatchKeyField{
			Value:  input.MatchKeys.ZIP.Value[0:5],
			Source: input.MatchKeys.ZIP.Source,
		}
	}

	existingCheck := 0
	if input.Signature.FiberType == "default" {
		existingCheck = GetRedisIntValue([]string{input.Signature.EventID, input.Signature.RecordID, "fiber"})
		if existingCheck == 1 { // this fiber has already been processed
			LogDev(fmt.Sprintf("Duplicate fiber detected %v", input.Signature))
			return nil
		}
	} else if input.Signature.FiberType == "mar" {
		existingCheck = GetRedisIntValue([]string{input.Signature.EventID, input.Signature.RecordID, "fiber"})
		if existingCheck == 0 { // default fiber has not been processed
			IncrRedisValue([]string{input.Signature.EventID, input.Signature.RecordID, "fiber-mar-retry"})
			retryCount := GetRedisIntValue([]string{input.Signature.EventID, input.Signature.RecordID, "fiber-mar-retry"})
			if retryCount < 30 {
				return fmt.Errorf("Default fiber not yet processed, retryn count  %v < max of 30, wait for retry", retryCount)
			}
		}
	}

	// store the fiber
	OutputPassthrough := ConvertPassthrough(input.Passthrough)
	var fiber PeopleFiber
	fiber.CreatedAt = time.Now()
	fiber.ID = uuid.New().String()
	fiber.MatchKeys = input.MatchKeys
	fiber.Passthrough = OutputPassthrough
	fiber.Signature = input.Signature

	// fiber in DS
	dsFiber := GetFiberDS(&fiber)
	dsNameSpace := strings.ToLower(fmt.Sprintf("%v-%v", Env, input.Signature.OwnerID))
	dsKey := datastore.NameKey(DSKindFiber, fiber.ID, nil)
	dsKey.Namespace = dsNameSpace
	dsFiber.ID = dsKey

	matchable := false
	if input.Signature.FiberType == "default" {
		if len(input.MatchKeys.EMAIL.Value) > 0 ||
			(len(input.MatchKeys.PHONE.Value) > 0 && len(input.MatchKeys.FINITIAL.Value) > 0) ||
			(len(input.MatchKeys.CITY.Value) > 0 &&
				(len(input.MatchKeys.STATE.Value) > 0 || (len(input.MatchKeys.COUNTRY.Value) > 0 && input.MatchKeys.COUNTRY.Value != "US")) &&
				len(input.MatchKeys.LNAME.Value) > 0 &&
				len(input.MatchKeys.FNAME.Value) > 0 &&
				len(input.MatchKeys.AD1.Value) > 0 &&
				len(input.MatchKeys.ADBOOK.Value) > 0) {
			matchable = true
		}
	} else {
		// MAR and MPR are matchable always
		matchable = true
	}

	HasNewValues := false
	var output People360Output
	var FiberSignatures []Signature
	output.ID = uuid.New().String()
	MatchKeyList := structs.Names(&PeopleOutput{})
	FiberMatchKeys := make(map[string][]string)
	// collect all fiber match key values
	for _, name := range MatchKeyList {
		FiberMatchKeys[name] = []string{}
	}
	var matchedFibers []string
	matchedDefaultFiber := 0
	var expiredSetCollection []string

	if matchable {

		// locate existing set
		if len(input.Signature.RecordID) == 0 {
			// ensure record id is not blank or we'll have problem
			input.Signature.RecordID = uuid.New().String()
		}
		MatchByValue0 := input.Signature.RecordID

		MatchByKey1 := "TRUSTEDID"
		MatchByValue1 := strings.Replace(input.MatchKeys.TRUSTEDID.Value, "'", `''`, -1)

		MatchByKey2 := "EMAIL"
		MatchByValue2 := strings.Replace(input.MatchKeys.EMAIL.Value, "'", `''`, -1)

		MatchByKey3A := "PHONE"
		MatchByValue3A := strings.Replace(input.MatchKeys.PHONE.Value, "'", `''`, -1)
		MatchByKey3B := "FINITIAL"
		MatchByValue3B := strings.Replace(input.MatchKeys.FINITIAL.Value, "'", `''`, -1)

		MatchByKey5A := "CITY"
		MatchByValue5A := strings.Replace(input.MatchKeys.CITY.Value, "'", `''`, -1)
		MatchByKey5B := "STATE"
		MatchByValue5B := strings.Replace(input.MatchKeys.STATE.Value, "'", `''`, -1)
		MatchByKey5C := "LNAME"
		MatchByValue5C := strings.Replace(input.MatchKeys.LNAME.Value, "'", `''`, -1)
		MatchByKey5D := "FNAME"
		MatchByValue5D := strings.Replace(input.MatchKeys.FNAME.Value, "'", `''`, -1)
		MatchByKey5E := "AD1"
		MatchByValue5E := strings.Replace(input.MatchKeys.AD1.Value, "'", `''`, -1)
		MatchByKey5F := "ADBOOK"
		MatchByValue5F := strings.Replace(input.MatchKeys.ADBOOK.Value, "'", `''`, -1)

		matchedSets := []PeopleSetDS{}
		queriedSets := []PeopleSetDS{}

		setQuery := datastore.NewQuery(DSKindSet).Namespace(dsNameSpace).Filter("recordid =", MatchByValue0)
		if _, err := ds.GetAll(ctx, setQuery, &queriedSets); err != nil {
			log.Fatalf("Error querying sets query 1: %v", err)
		} else {
			for _, s := range queriedSets {
				matchedSets = append(matchedSets, s)
			}
			LogDev(fmt.Sprintf("Matched %v sets with condition 1", len(queriedSets)))
		}
		if len(MatchByValue1) > 0 {
			setQuery := datastore.NewQuery(DSKindSet).Namespace(dsNameSpace).Filter(strings.ToLower(MatchByKey1)+"normalized =", strings.ToUpper(MatchByValue1))
			if _, err := ds.GetAll(ctx, setQuery, &queriedSets); err != nil {
				log.Fatalf("Error querying sets query 1: %v", err)
			} else {
				for _, s := range queriedSets {
					matchedSets = append(matchedSets, s)
				}
				LogDev(fmt.Sprintf("Matched %v sets with condition 2", len(queriedSets)))
			}
		}
		if len(MatchByValue2) > 0 {
			setQuery := datastore.NewQuery(DSKindSet).Namespace(dsNameSpace).Filter(strings.ToLower(MatchByKey2)+"normalized =", strings.ToUpper(MatchByValue2))
			if _, err := ds.GetAll(ctx, setQuery, &queriedSets); err != nil {
				log.Fatalf("Error querying sets query 1: %v", err)
			} else {
				for _, s := range queriedSets {
					matchedSets = append(matchedSets, s)
				}
				LogDev(fmt.Sprintf("Matched %v sets with condition 3", len(queriedSets)))
			}
		}
		if len(MatchByValue3A) > 0 && len(MatchByValue3B) > 0 {
			setQuery := datastore.NewQuery(DSKindSet).Namespace(dsNameSpace).
				Filter(strings.ToLower(MatchByKey3A)+"normalized =", strings.ToUpper(MatchByValue3A)).
				Filter(strings.ToLower(MatchByKey3B)+"normalized =", strings.ToUpper(MatchByValue3B))
			if _, err := ds.GetAll(ctx, setQuery, &queriedSets); err != nil {
				log.Fatalf("Error querying sets query 1: %v", err)
			} else {
				for _, s := range queriedSets {
					matchedSets = append(matchedSets, s)
				}
				LogDev(fmt.Sprintf("Matched %v sets with condition 4", len(queriedSets)))
			}
		}
		if len(MatchByValue5A) > 0 && len(MatchByValue5B) > 0 && len(MatchByValue5C) > 0 && len(MatchByValue5D) > 0 && len(MatchByValue5E) > 0 && len(MatchByValue5F) > 0 {
			setQuery := datastore.NewQuery(DSKindSet).Namespace(dsNameSpace).
				Filter(strings.ToLower(MatchByKey5A)+"normalized =", strings.ToUpper(MatchByValue5A)).
				Filter(strings.ToLower(MatchByKey5B)+"normalized =", strings.ToUpper(MatchByValue5B)).
				Filter(strings.ToLower(MatchByKey5C)+"normalized =", strings.ToUpper(MatchByValue5C)).
				Filter(strings.ToLower(MatchByKey5D)+"normalized =", strings.ToUpper(MatchByValue5D)).
				Filter(strings.ToLower(MatchByKey5E)+"normalized =", strings.ToUpper(MatchByValue5E)).
				Filter(strings.ToLower(MatchByKey5F)+"normalized =", strings.ToUpper(MatchByValue5F))
			if _, err := ds.GetAll(ctx, setQuery, &queriedSets); err != nil {
				log.Fatalf("Error querying sets query 1: %v", err)
			} else {
				for _, s := range queriedSets {
					matchedSets = append(matchedSets, s)
				}
				LogDev(fmt.Sprintf("Matched %v sets with condition 5", len(queriedSets)))
			}
		} else {
			LogDev(fmt.Sprintf("condition 5 not triggered: %v, %v, %v, %v, %v, %v", MatchByValue5A, MatchByValue5B, MatchByValue5C, MatchByValue5D, MatchByValue5E, MatchByValue5F))
		}

		for _, s := range matchedSets {
			if !Contains(expiredSetCollection, s.ID.Name) {
				expiredSetCollection = append(expiredSetCollection, s.ID.Name)
			}
			if len(s.Fibers) > 0 {
				for _, f := range s.Fibers {
					if !Contains(matchedFibers, f) {
						matchedFibers = append(matchedFibers, f)
					}
				}
			}
		}

		LogDev(fmt.Sprintf("Fiber Collection: %v, Matched Set Collection: %v", matchedFibers, matchedSets))
		LogDev(fmt.Sprintf("Expired Sets: %v", expiredSetCollection))

		// get all the Fibers
		var FiberKeys []*datastore.Key
		var Fibers []PeopleFiberDS
		for _, fiber := range matchedFibers {
			dsFiberGetKey := datastore.NameKey(DSKindFiber, fiber, nil)
			dsFiberGetKey.Namespace = dsNameSpace
			FiberKeys = append(FiberKeys, dsFiberGetKey)
			Fibers = append(Fibers, PeopleFiberDS{})
		}
		if len(FiberKeys) > 0 {
			if err := ds.GetMulti(ctx, FiberKeys, Fibers); err != nil && err != datastore.ErrNoSuchEntity {
				log.Fatalf("Error fetching fibers ns %v kind %v, keys %v: %v,", dsNameSpace, DSKindFiber, FiberKeys, err)
			}
		}

		// sort by createdAt desc
		sort.Slice(Fibers, func(i, j int) bool {
			return Fibers[i].CreatedAt.After(Fibers[j].CreatedAt)
		})

		LogDev(fmt.Sprintf("Fibers: %v", Fibers))

		for i, fiber := range Fibers {
			LogDev(fmt.Sprintf("loaded fiber %v of %v: %v", i, len(Fibers), fiber))
			FiberSignatures = append(FiberSignatures, Signature{
				OwnerID:   fiber.OwnerID,
				Source:    fiber.Source,
				EventType: fiber.EventType,
				EventID:   fiber.EventID,
				RecordID:  fiber.RecordID,
			})

			if fiber.FiberType == "default" {
				matchedDefaultFiber++
			}

			for _, name := range MatchKeyList {
				value := strings.TrimSpace(GetMatchKeyFieldFromDSFiber(&fiber, name).Value)
				if len(value) > 0 && !Contains(FiberMatchKeys[name], value) {
					FiberMatchKeys[name] = append(FiberMatchKeys[name], value)
				}
			}
			LogDev(fmt.Sprintf("FiberMatchKey values %v", FiberMatchKeys))
		}
		var MatchKeysFromFiber []MatchKey360

		// check to see if there are any new values
		for _, name := range MatchKeyList {
			mk360 := MatchKey360{
				Key:    name,
				Values: FiberMatchKeys[name],
			}

			newValue := strings.TrimSpace(GetMatchKeyFieldFromStruct(&input.MatchKeys, name).Value)
			if len(newValue) > 0 {
				if !Contains(mk360.Values, newValue) {
					LogDev(fmt.Sprintf("new values found %v, %v for key %v, chars are %v", mk360.Values, newValue, name, ToAsciiArray(newValue)))
					HasNewValues = true
				}
			}

			MatchKeysFromFiber = append(MatchKeysFromFiber, mk360)
			// LogDev(fmt.Sprintf("mk.Values %v: %v", name, FiberMatchKeys[name]))
		}

		output.MatchKeys = MatchKeysFromFiber

	}
	if !matchable {
		dsFiber.Disposition = "purge"
	} else if matchedDefaultFiber == 0 {
		dsFiber.Disposition = "new"
	} else if !HasNewValues {
		dsFiber.Disposition = "dupe"
	} else {
		dsFiber.Disposition = "update"
	}

	// store the fiber
	if _, err := ds.Put(ctx, dsKey, &dsFiber); err != nil {
		log.Fatalf("Error: storing Fiber sig %v, error %v", input.Signature, err)
	}

	// stop processing if no new values
	// if !HasNewValues {
	// 	return nil
	// }
	if !matchable {
		LogDev(fmt.Sprintf("Unmatchable fiber detected %v", input.Signature))
		IncrRedisValue([]string{input.Signature.EventID, "fibers-deleted"})
		return nil
	}

	// append to the output value
	output.Signatures = append(FiberSignatures, input.Signature)
	output.Signature = Signature360{
		OwnerID:   input.Signature.OwnerID,
		Source:    input.Signature.Source,
		EventID:   input.Signature.EventID,
		EventType: input.Signature.EventType,
	}
	if output.CreatedAt.IsZero() {
		output.CreatedAt = time.Now()
	}
	output.Fibers = append(matchedFibers, fiber.ID)
	output.Passthroughs = OutputPassthrough
	//output.TrustedIDs = append(output.TrustedIDs, input.MatchKeys.CAMPAIGNID.Value)
	var OutputMatchKeys []MatchKey360
	for _, name := range MatchKeyList {
		mk := GetMatchKey360ByName(output.MatchKeys, name)
		mk.Key = name
		mk.Value = strings.TrimSpace(GetMatchKeyFieldFromStruct(&input.MatchKeys, name).Value)
		// if blank, assign it a value
		if len(mk.Value) == 0 && len(mk.Values) > 0 {
			mk.Value = mk.Values[0]
		}
		if len(mk.Value) > 0 && !Contains(mk.Values, mk.Value) {
			mk.Values = append(mk.Values, mk.Value)
		}

		// special rules for assigning values
		if name == "TITLE" {
			mk.Value = GetSmallestYear(mk.Values)
		}

		OutputMatchKeys = append(OutputMatchKeys, *mk)
	}
	output.MatchKeys = OutputMatchKeys

	// record the set id in DS
	var setDS PeopleSetDS
	setKey := datastore.NameKey(DSKindSet, output.ID, nil)
	setKey.Namespace = dsNameSpace
	setDS.ID = setKey
	setDS.Fibers = output.Fibers
	setDS.CreatedAt = output.CreatedAt
	PopulateSetOutputSignatures(&setDS, output.Signatures)
	PopulateSetOutputMatchKeys(&setDS, output.MatchKeys)
	if _, err := ds.Put(ctx, setKey, &setDS); err != nil {
		log.Printf("Error: storing set with sig %v, error %v", input.Signature, err)
	}

	var goldenDS PeopleGoldenDS
	goldenKey := datastore.NameKey(DSKindGolden, output.ID, nil)
	goldenKey.Namespace = dsNameSpace
	goldenDS.ID = goldenKey
	goldenDS.CreatedAt = output.CreatedAt
	PopulateGoldenOutputMatchKeys(&goldenDS, output.MatchKeys)
	if _, err := ds.Put(ctx, goldenKey, &goldenDS); err != nil {
		log.Printf("Error: storing golden record with sig %v, error %v", input.Signature, err)
	}

	// remove expired sets and setmembers from DS
	var SetKeys []*datastore.Key
	// var MemberKeys []*datastore.Key
	var GoldenKeys []*datastore.Key

	for _, set := range expiredSetCollection {
		setKey := datastore.NameKey(DSKindSet, set, nil)
		setKey.Namespace = dsNameSpace
		SetKeys = append(SetKeys, setKey)
		goldenKey := datastore.NameKey(DSKindGolden, set, nil)
		goldenKey.Namespace = dsNameSpace
		GoldenKeys = append(GoldenKeys, goldenKey)
	}
	LogDev(fmt.Sprintf("deleting %v expired sets and %v expired golden records", len(SetKeys), len(GoldenKeys)))
	if err := ds.DeleteMulti(ctx, SetKeys); err != nil {
		log.Printf("Error: deleting expired sets: %v", err)
	}
	if err := ds.DeleteMulti(ctx, GoldenKeys); err != nil {
		log.Printf("Error: deleting expired golden records: %v", err)
	}

	if input.Signature.FiberType == "default" {
		IncrRedisValue([]string{input.Signature.EventID, "fibers-completed"})
		SetRedisKeyWithExpiration([]string{input.Signature.EventID, input.Signature.RecordID, "fiber"})

		// grab the count and see if we are done
		counters := GetRedisIntValues([][]string{
			[]string{input.Signature.EventID, "records-total"},
			[]string{input.Signature.EventID, "records-completed"},
			[]string{input.Signature.EventID, "records-deleted"},
			[]string{input.Signature.EventID, "fibers-completed"},
			[]string{input.Signature.EventID, "fibers-deleted"},
		})
		LogDev(fmt.Sprintf("Received response from redis %v", counters))
		recordCount, recordCompleted, recordDeleted, fiberCompleted, fiberDeleted := 0, 0, 0, 0, 0
		if len(counters) == 5 {
			recordCount = counters[0]
			recordCompleted = counters[1]
			recordDeleted = counters[2]
			fiberCompleted = counters[3]
			fiberDeleted = counters[4]
		}
		recordFinished := false
		fiberFinished := false
		if recordCompleted+recordDeleted >= recordCount && recordCount > 0 {
			recordFinished = true
		}
		if fiberCompleted+fiberDeleted >= recordCount && recordCount > 0 {
			fiberFinished = true
		}
		LogDev(fmt.Sprintf("record finished ? %v; fiber finished ? %v", recordFinished, fiberFinished))
		if recordFinished && fiberFinished {
			eventData := EventData{
				Signature: input.Signature,
				EventData: make(map[string]interface{}),
			}
			eventData.EventData["status"] = "Finished"
			eventData.EventData["message"] = fmt.Sprintf("Processed %v records and %v fibers, purged %v records and %v fibers", recordCompleted, fiberCompleted, recordDeleted, fiberDeleted)
			eventData.EventData["records-total"] = recordCount
			eventData.EventData["records-completed"] = recordCompleted
			eventData.EventData["records-deleted"] = recordDeleted
			eventData.EventData["fibers-completed"] = fiberCompleted
			eventData.EventData["fibers-deleted"] = fiberDeleted
			statusJSON, _ := json.Marshal(eventData)
			psresult := status.Publish(ctx, &pubsub.Message{
				Data: statusJSON,
			})
			_, err := psresult.Get(ctx)
			if err != nil {
				log.Fatalf("%v Could not pub status to pubsub: %v", input.Signature.EventID, err)
			}
		}
	} else if input.Signature.FiberType == "mar" {
		SetRedisKeyWithExpiration([]string{input.Signature.EventID, input.Signature.RecordID, "fiber-mar"})
	}

	// push into pubsub
	outputJSON, _ := json.Marshal(output)
	psresult := topic.Publish(ctx, &pubsub.Message{
		Data: outputJSON,
		Attributes: map[string]string{
			"type":   "people",
			"source": "360",
		},
	})
	psid, err := psresult.Get(ctx)
	_, err = psresult.Get(ctx)
	if err != nil {
		log.Printf("Error: %v Could not pub to pubsub: %v", input.Signature.EventID, err)
	} else {
		LogDev(fmt.Sprintf("%v pubbed record as message id %v: %v", input.Signature.EventID, psid, string(outputJSON)))
	}

	topic2.Publish(ctx, &pubsub.Message{
		Data: outputJSON,
		Attributes: map[string]string{
			"type":   "people",
			"source": "360",
		},
	})

	return nil
}

func GetSmallestYear(values []string) string {
	if len(values) == 0 {
		return ""
	}
	eligible := []string{}
	for _, v := range values {
		if strings.HasPrefix(v, "20") && len(v) == 4 && IsInt(v) {
			eligible = append(eligible, v)
		}
	}
	if len(eligible) > 0 {
		sort.Strings(eligible)
		return eligible[0]
	} else {
		return ""
	}
}

func GetMatchKeyFieldFromStruct(v *PeopleOutput, field string) MatchKeyField {
	r := reflect.ValueOf(v)
	f := reflect.Indirect(r).FieldByName(field)
	return f.Interface().(MatchKeyField)
}

func GetMatchKeyFieldFromDSFiber(v *PeopleFiberDS, field string) MatchKeyField {
	r := reflect.ValueOf(v)
	f := reflect.Indirect(r).FieldByName(field)
	return f.Interface().(MatchKeyField)
}

func GetMatchKey360ByName(v []MatchKey360, key string) *MatchKey360 {
	for _, m := range v {
		if m.Key == key {
			return &m
		}
	}
	return &MatchKey360{}
}

func Contains(slice []string, item string) bool {
	for _, v := range slice {
		if strings.EqualFold(v, item) {
			return true
		}
	}
	return false
}

func ConvertPassthrough(v map[string]string) []Passthrough360 {
	var result []Passthrough360
	if len(v) > 0 {
		for mapKey, mapValue := range v {
			pt := Passthrough360{
				Name:  mapKey,
				Value: mapValue,
			}
			result = append(result, pt)
		}
	}
	return result
}

func GetFiberDS(v *PeopleFiber) PeopleFiberDS {
	p := PeopleFiberDS{
		OwnerID:     v.Signature.OwnerID,
		Source:      v.Signature.Source,
		EventType:   v.Signature.EventType,
		EventID:     v.Signature.EventID,
		RecordID:    v.Signature.RecordID,
		FiberType:   v.Signature.FiberType,
		Passthrough: v.Passthrough,
		CreatedAt:   v.CreatedAt,
	}
	PopulateFiberMatchKeys(&p, &(v.MatchKeys))
	return p
}

func GetSignatureField(v *Signature, field string) string {
	r := reflect.ValueOf(v)
	f := reflect.Indirect(r).FieldByName(field)
	return f.Interface().(string)
}

func GetSignatureSliceValues(source []Signature, field string) []string {
	slice := []string{}
	for _, s := range source {
		slice = append(slice, GetSignatureField(&s, field))
	}
	return slice
}

func GetRecordIDNormalizedSliceValues(source []Signature, field string) []string {
	slice := []string{}
	for _, s := range source {
		slice = append(slice, Left(GetSignatureField(&s, field), 36))
	}
	return slice
}

func SetPeople360SetOutputFieldValues(v *PeopleSetDS, field string, value []string) {
	r := reflect.ValueOf(v)
	f := reflect.Indirect(r).FieldByName(field)
	f.Set(reflect.ValueOf(value))
	// LogDev(fmt.Sprintf("SetPeople360SetOutputFieldValues: %v %v", field, value))
}

func SetPeople360GoldenOutputFieldValue(v *PeopleGoldenDS, field string, value string) {
	r := reflect.ValueOf(v)
	f := reflect.Indirect(r).FieldByName(field)
	f.Set(reflect.ValueOf(value))
	// LogDev(fmt.Sprintf("SetPeople360GoldenOutputFieldValue: %v %v", field, value))
}

func SetPeopleFiberMatchKeyField(v *PeopleFiberDS, field string, value MatchKeyField) {
	LogDev(fmt.Sprintf("SetPeopleFiberMatchKeyField: %v %v", field, value))
	r := reflect.ValueOf(v)
	f := reflect.Indirect(r).FieldByName(field)
	f.Set(reflect.ValueOf(value))

}

func PopulateSetOutputSignatures(target *PeopleSetDS, values []Signature) {
	KeyList := structs.Names(&Signature{})
	for _, key := range KeyList {
		SetPeople360SetOutputFieldValues(target, key, GetSignatureSliceValues(values, key))
		if key == "RecordID" {
			SetPeople360SetOutputFieldValues(target, key+"Normalized", GetRecordIDNormalizedSliceValues(values, key))
		}
	}
}

func PopulateFiberMatchKeys(target *PeopleFiberDS, source *PeopleOutput) {
	KeyList := structs.Names(&PeopleOutput{})
	for _, key := range KeyList {
		SetPeopleFiberMatchKeyField(target, key, GetMatchKeyFieldFromStruct(source, key))
	}
}

func PopulateSetOutputMatchKeys(target *PeopleSetDS, values []MatchKey360) {
	KeyList := structs.Names(&PeopleOutput{})
	for _, key := range KeyList {
		SetPeople360SetOutputFieldValues(target, key, GetSetValuesFromMatchKeys(values, key))
		SetPeople360SetOutputFieldValues(target, key+"Normalized", GetSetValuesFromMatchKeysNormalized(values, key))
	}
}

func PopulateGoldenOutputMatchKeys(target *PeopleGoldenDS, values []MatchKey360) {
	KeyList := structs.Names(&PeopleOutput{})
	for _, key := range KeyList {
		SetPeople360GoldenOutputFieldValue(target, key, GetGoldenValueFromMatchKeys(values, key))
	}
}

func GetGoldenValueFromMatchKeys(values []MatchKey360, key string) string {
	for _, m := range values {
		if m.Key == key {
			return m.Value
		}
	}
	return ""
}

func GetSetValuesFromMatchKeys(values []MatchKey360, key string) []string {
	for _, m := range values {
		if m.Key == key {
			return m.Values
		}
	}
	return []string{}
}

func GetSetValuesFromMatchKeysNormalized(values []MatchKey360, key string) []string {
	result := []string{}
	for _, m := range values {
		if m.Key == key {
			for _, v := range m.Values {
				result = append(result, strings.ToUpper(v))
			}
			return result
		}
	}
	return []string{}
}

func LogDev(s string) {
	if dev {
		log.Printf(s)
	}
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

func ToAsciiArray(s string) []int {
	runes := []rune(s)

	var result []int

	for i := 0; i < len(runes); i++ {
		result = append(result, int(runes[i]))
	}
	return result
}

func IsInt(s string) bool {
	for _, c := range s {
		if !unicode.IsDigit(c) {
			return false
		}
	}
	return true
}

func SetRedisValueWithExpiration(keyparts []string, value int) {
	ms := msp.Get()
	defer ms.Close()

	_, err := ms.Do("SETEX", strings.Join(keyparts, ":"), redisTransientExpiration, value)
	if err != nil {
		log.Printf("Error setting redis value %v to %v, error %v", strings.Join(keyparts, ":"), value, err)
	}
}

func IncrRedisValue(keyparts []string) { // no need to update expiration
	ms := msp.Get()
	defer ms.Close()

	_, err := ms.Do("INCR", strings.Join(keyparts, ":"))
	if err != nil {
		log.Printf("Error incrementing redis value %v, error %v", strings.Join(keyparts, ":"), err)
	}
}

func SetRedisKeyWithExpiration(keyparts []string) {
	SetRedisValueWithExpiration(keyparts, 1)
}

func GetRedisIntValue(keyparts []string) int {
	ms := msp.Get()
	defer ms.Close()
	value, err := redis.Int(ms.Do("GET", strings.Join(keyparts, ":")))
	if err != nil {
		log.Printf("Error getting redis value %v, error %v", strings.Join(keyparts, ":"), err)
	}
	return value
}

func GetRedisIntValues(keys [][]string) []int {
	ms := msp.Get()
	defer ms.Close()

	formattedKeys := []string{}
	for _, key := range keys {
		formattedKeys = append(formattedKeys, strings.Join(key, ":"))
	}

	values, err := redis.Ints(ms.Do("MGET", formattedKeys[0], formattedKeys[1], formattedKeys[2], formattedKeys[3], formattedKeys[4]))
	if err != nil {
		log.Printf("Error getting redis values %v, error %v", formattedKeys, err)
	}
	return values
}
