package people360

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/datastore"
	"cloud.google.com/go/pubsub"

	"github.com/fatih/structs"
	"github.com/google/uuid"
	"google.golang.org/api/iterator"

	"github.com/elastic/go-elasticsearch/v6"
	"github.com/elastic/go-elasticsearch/v6/esapi"
)

type PubSubMessage struct {
	Data []byte `json:"data"`
}

type Signature struct {
	OwnerID   string `json:"ownerId" bigquery:"ownerid"`
	Source    string `json:"source" bigquery:"source"`
	EventID   string `json:"eventId" bigquery:"eventid"`
	EventType string `json:"eventType" bigquery:"eventtype"`
	RecordID  string `json:"recordId" bigquery:"recordid"`
}

type PeopleInput struct {
	Signature   Signature         `json:"signature"`
	Passthrough map[string]string `json:"passthrough"`
	MatchKeys   PeopleOutput      `json:"matchkeys`
	// MatchKeys   map[PeopleOutput]PeopleOutput      `json:"matchkeys`
}

type PeopleFiber struct {
	Signature Signature `json:"signature" bigquery:"signature"`
	// Passthrough map[string]string `json:"passthrough" bigquery:"passthrough"`
	Passthrough []Passthrough360 `json:"passthrough" bigquery:"passthrough"`
	MatchKeys   PeopleOutput     `json:"matchkeys" bigquery:"matchkeys"`
	FiberID     string           `json:"fiberId" bigquery:"id"`
	CreatedAt   time.Time        `json:"createdAt" bigquery:"createdAt"`
}

type PeopleFiberDS struct {
	OwnerID     string           `json:"ownerId" datastore:"ownerid"`
	Source      string           `json:"source" datastore:"source"`
	EventID     string           `json:"eventId" datastore:"eventid"`
	EventType   string           `json:"eventType" datastore:"eventtype"`
	RecordID    string           `json:"recordId" datastore:"recordid"`
	Passthrough []Passthrough360 `json:"passthrough" datastore:"passthrough"`
	MatchKeys   PeopleOutput     `json:"matchkeys" datastore:"matchkeys"`
	FiberID     *datastore.Key   `datastore:"__key__"`
	CreatedAt   time.Time        `json:"createdAt" datastore:"createdAt"`
}

type PeopleSetMember struct {
	SetID     string
	OwnerID   string
	Source    string
	EventID   string
	EventType string
	RecordID  string
	FiberID   string
	ID        *datastore.Key `datastore:"__key__"`
}

type MatchKeyField struct {
	Value  string `json:"value" bigquery:"value"`
	Source string `json:"source" bigquery:"source"`
	Type   string `json:"type" bigquery:"type"`
}

type PeopleOutput struct {
	SALUTATION MatchKeyField `json:"salutation" bigquery:"salutation"`
	NICKNAME   MatchKeyField `json:"nickname" bigquery:"nickname"`
	FNAME      MatchKeyField `json:"fname" bigquery:"fname"`
	FINITIAL   MatchKeyField `json:"finitial" bigquery:"finitial"`
	LNAME      MatchKeyField `json:"lname" bigquery:"lname"`
	MNAME      MatchKeyField `json:"mname" bigquery:"mname"`

	AD1       MatchKeyField `json:"ad1" bigquery:"ad1"`
	AD1NO     MatchKeyField `json:"ad1no" bigquery:"ad1no"`
	AD2       MatchKeyField `json:"ad2" bigquery:"ad2"`
	AD3       MatchKeyField `json:"ad3" bigquery:"ad3"`
	CITY      MatchKeyField `json:"city" bigquery:"city"`
	STATE     MatchKeyField `json:"state" bigquery:"state"`
	ZIP       MatchKeyField `json:"zip" bigquery:"zip"`
	ZIP5      MatchKeyField `json:"zip5" bigquery:"zip5"`
	COUNTRY   MatchKeyField `json:"country" bigquery:"country"`
	MAILROUTE MatchKeyField `json:"mailroute" bigquery:"mailroute"`
	ADTYPE    MatchKeyField `json:"adtype" bigquery:"adtype"`
	ADPARSER  MatchKeyField `json:"adparser" bigquery:"adparser"`
	ADCORRECT MatchKeyField `json:"adcorrect" bigquery:"adcorrect"`

	EMAIL MatchKeyField `json:"email" bigquery:"email"`
	PHONE MatchKeyField `json:"phone" bigquery:"phone"`

	TRUSTEDID MatchKeyField `json:"trustedId" bigquery:"trustedid"`
	CLIENTID  MatchKeyField `json:"clientId" bigquery:"clientid"`

	GENDER MatchKeyField `json:"gender" bigquery:"gender"`
	AGE    MatchKeyField `json:"age" bigquery:"age"`
	DOB    MatchKeyField `json:"dob" bigquery:"dob"`

	ORGANIZATION MatchKeyField `json:"organization" bigquery:"organization"`
	TITLE        MatchKeyField `json:"title" bigquery:"title"`
	ROLE         MatchKeyField `json:"role" bigquery:"role"`
	STATUS       MatchKeyField `json:"status" bigquery:"status"`
}

type Signature360 struct {
	OwnerID   string `json:"ownerId" bigquery:"ownerId"`
	Source    string `json:"source" bigquery:"source"`
	EventID   string `json:"eventId" bigquery:"eventId"`
	EventType string `json:"eventType" bigquery:"eventType"`
}

type MatchKey360 struct {
	Key    string   `json:"key" bigquery:"key"`
	Type   string   `json:"type" bigquery:"type"`
	Value  string   `json:"value" bigquery:"value"`
	Values []string `json:"values" bigquery:"values"`
}

type Passthrough360 struct {
	Name  string `json:"name" bigquery:"name"`
	Value string `json:"value" bigquery:"value"`
}

type People360Output struct {
	ID           string           `json:"id" bigquery:"id"`
	Signature    Signature360     `json:"signature" bigquery:"signature"`
	Signatures   []Signature      `json:"signatures" bigquery:"signatures"`
	CreatedAt    time.Time        `json:"createdAt" bigquery:"createdAt"`
	Fibers       []string         `json:"fibers" bigquery:"fibers"`
	Passthroughs []Passthrough360 `json:"passthroughs" bigquery:"passthroughs"`
	MatchKeys    []MatchKey360    `json:"matchKeys" bigquery:"matchKeys"`
}

type People360OutputDS struct {
	ID           string    `datastore:"id"`
	OwnerID      []string  `datastore:"sig.ownerId"`
	Source       []string  `datastore:"sig.source"`
	EventID      []string  `datastore:"sig.eventId"`
	EventType    []string  `datastore:"sig.eventType"`
	RecordID     []string  `datastore:"sig.recordId"`
	CreatedAt    time.Time `datastore:"createdat"`
	Fibers       []string  `datastore:"fibers"`
	SALUTATION   []string  `datastore:"mk.salutation"`
	NICKNAME     []string  `datastore:"mk.nickname"`
	FNAME        []string  `datastore:"mk.fname"`
	FINITIAL     []string  `datastore:"mk.finitial"`
	LNAME        []string  `datastore:"mk.lname"`
	MNAME        []string  `datastore:"mk.mname"`
	AD1          []string  `datastore:"mk.ad1"`
	AD1NO        []string  `datastore:"mk.ad1no"`
	AD2          []string  `datastore:"mk.ad2"`
	AD3          []string  `datastore:"mk.ad3"`
	CITY         []string  `datastore:"mk.city"`
	STATE        []string  `datastore:"mk.state"`
	ZIP          []string  `datastore:"mk.zip"`
	ZIP5         []string  `datastore:"mk.zip5"`
	COUNTRY      []string  `datastore:"mk.country"`
	MAILROUTE    []string  `datastore:"mk.mailroute"`
	ADTYPE       []string  `datastore:"mk.adtype"`
	ADPARSER     []string  `datastore:"mk.adparser"`
	ADCORRECT    []string  `datastore:"mk.adcorrect"`
	EMAIL        []string  `datastore:"mk.email"`
	PHONE        []string  `datastore:"mk.phone"`
	TRUSTEDID    []string  `datastore:"mk.trustedid"`
	CLIENTID     []string  `datastore:"mk.clientid"`
	GENDER       []string  `datastore:"mk.gender"`
	AGE          []string  `datastore:"mk.age"`
	DOB          []string  `datastore:"mk.dob"`
	ORGANIZATION []string  `datastore:"mk.organization"`
	TITLE        []string  `datastore:"mk.title"`
	ROLE         []string  `datastore:"mk.role"`
	STATUS       []string  `datastore:"mk.status"`
}

type People360GoldenDS struct {
	ID           string    `datastore:"id"`
	CreatedAt    time.Time `datastore:"createdat"`
	SALUTATION   string    `datastore:"mk.salutation"`
	NICKNAME     string    `datastore:"mk.nickname"`
	FNAME        string    `datastore:"mk.fname"`
	FINITIAL     string    `datastore:"mk.finitial"`
	LNAME        string    `datastore:"mk.lname"`
	MNAME        string    `datastore:"mk.mname"`
	AD1          string    `datastore:"mk.ad1"`
	AD1NO        string    `datastore:"mk.ad1no"`
	AD2          string    `datastore:"mk.ad2"`
	AD3          string    `datastore:"mk.ad3"`
	CITY         string    `datastore:"mk.city"`
	STATE        string    `datastore:"mk.state"`
	ZIP          string    `datastore:"mk.zip"`
	ZIP5         string    `datastore:"mk.zip5"`
	COUNTRY      string    `datastore:"mk.country"`
	MAILROUTE    string    `datastore:"mk.mailroute"`
	ADTYPE       string    `datastore:"mk.adtype"`
	ADPARSER     string    `datastore:"mk.adparser"`
	ADCORRECT    string    `datastore:"mk.adcorrect"`
	EMAIL        string    `datastore:"mk.email"`
	PHONE        string    `datastore:"mk.phone"`
	TRUSTEDID    string    `datastore:"mk.trustedid"`
	CLIENTID     string    `datastore:"mk.clientid"`
	GENDER       string    `datastore:"mk.gender"`
	AGE          string    `datastore:"mk.age"`
	DOB          string    `datastore:"mk.dob"`
	ORGANIZATION string    `datastore:"mk.organization"`
	TITLE        string    `datastore:"mk.title"`
	ROLE         string    `datastore:"mk.role"`
	STATUS       string    `datastore:"mk.status"`
}

var ProjectID = os.Getenv("PROJECTID")
var PubSubTopic = os.Getenv("PSOUTPUT")
var PubSubTopic2 = os.Getenv("PSOUTPUT2")
var SetTableName = os.Getenv("SETTABLE")
var FiberTableName = os.Getenv("FIBERTABLE")
var ESUrl = os.Getenv("ELASTICURL")
var ESUid = os.Getenv("ELASTICUSER")
var ESPwd = os.Getenv("ELASTICPWD")
var ESIndex = os.Getenv("ELASTICINDEX")
var Env = os.Getenv("ENVIRONMENT")
var dev = os.Getenv("ENVIRONMENT") == "dev"
var DSKindSet = os.Getenv("DSKINDSET")
var DSKindGolden = os.Getenv("DSKINDGOLDEN")
var DSKindFiber = os.Getenv("DSKINDFIBER")
var DSKindMember = os.Getenv("DSKINDMEMBER")

var reAlphaNumeric = regexp.MustCompile("[^a-zA-Z0-9]+")

var ps *pubsub.Client
var topic *pubsub.Topic
var topic2 *pubsub.Topic

var bq *bigquery.Client
var bs bigquery.Schema
var bc bigquery.Schema
var es *elasticsearch.Client
var ds *datastore.Client

// var setSchema bigquery.Schema

func init() {
	ctx := context.Background()
	ps, _ = pubsub.NewClient(ctx, ProjectID)
	ds, _ = datastore.NewClient(ctx, ProjectID)
	topic = ps.Topic(PubSubTopic)
	topic2 = ps.Topic(PubSubTopic2)
	bq, _ = bigquery.NewClient(ctx, ProjectID)
	bs, _ = bigquery.InferSchema(People360Output{})
	bc, _ = bigquery.InferSchema(PeopleFiber{})

	es, _ = elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{ESUrl},
		Username:  ESUid,
		Password:  ESPwd,
	})

	log.Printf("init completed, pubsub topic name: %v, bq client: %v, bq schema: %v, %v", topic, bq, bs, bc)
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
	if len(input.MatchKeys.ZIP.Value) > 0 {
		input.MatchKeys.ZIP5 = MatchKeyField{
			Value:  input.MatchKeys.ZIP.Value[0:5],
			Source: input.MatchKeys.ZIP.Source,
		}
	}

	// locate by key (trusted id)
	setMeta := &bigquery.TableMetadata{
		Schema: bs,
	}
	fiberMeta := &bigquery.TableMetadata{
		Schema: bc,
	}
	DatasetID := strings.ToLower(reAlphaNumeric.ReplaceAllString(Env+input.Signature.OwnerID, ""))
	// make sure dataset exists
	dsmeta := &bigquery.DatasetMetadata{
		Location: "US", // Create the dataset in the US.
	}
	if err := bq.Dataset(DatasetID).Create(ctx, dsmeta); err != nil {
	}
	SetTable := bq.Dataset(DatasetID).Table(SetTableName)
	if err := SetTable.Create(ctx, setMeta); err != nil {
		if !strings.Contains(err.Error(), "Already Exists") {
			log.Printf("error creating set table %v", err)
		}
	}
	FiberTable := bq.Dataset(DatasetID).Table(FiberTableName)
	if err := FiberTable.Create(ctx, fiberMeta); err != nil {
		if !strings.Contains(err.Error(), "Already Exists") {
			log.Printf("error creating fiber table %v", err)
		}
	}

	// store the fiber
	OutputPassthrough := ConvertPassthrough(input.Passthrough)
	var fiber PeopleFiber
	fiber.CreatedAt = time.Now()
	fiber.FiberID = uuid.New().String()
	fiber.MatchKeys = input.MatchKeys
	fiber.Passthrough = OutputPassthrough
	fiber.Signature = input.Signature

	// let's log this into ES
	_ = PersistInES(ctx, fiber)

	// store in BQ
	FiberInserter := FiberTable.Inserter()
	if err := FiberInserter.Put(ctx, fiber); err != nil {
		log.Fatalf("error insertinng into fiber table %v", err)
	}

	// store in DS
	dsNameSpace := strings.ToLower(fmt.Sprintf("%v-%v", Env, input.Signature.OwnerID))
	dsKey := datastore.NameKey(DSKindFiber, fiber.FiberID, nil)
	dsKey.Namespace = dsNameSpace
	dsFiber := GetFiberDS(&fiber)
	if _, err := ds.Put(ctx, dsKey, &dsFiber); err != nil {
		log.Fatalf("Exception storing Fiber sig %v, error %v", input.Signature, err)
	}

	// locate existing set

	if len(input.Signature.RecordID) == 0 {
		// ensure record id is not blank or we'll have problem
		input.Signature.RecordID = uuid.New().String()
	}
	MatchByValue0 := input.Signature.RecordID

	MatchByKey1 := "TRUSTEDID"
	MatchByValue1 := strings.Replace(input.MatchKeys.TRUSTEDID.Value, "'", `\'`, -1)

	MatchByKey2 := "EMAIL"
	MatchByValue2 := strings.Replace(input.MatchKeys.EMAIL.Value, "'", `\'`, -1)

	MatchByKey3A := "PHONE"
	MatchByValue3A := strings.Replace(input.MatchKeys.PHONE.Value, "'", `\'`, -1)
	MatchByKey3B := "FINITIAL"
	MatchByValue3B := strings.Replace(input.MatchKeys.FINITIAL.Value, "'", `\'`, -1)

	// MatchByKey4A := "ZIP5"
	// MatchByValue4A := strings.Replace(input.MatchKeys.ZIP5.Value, "'", `\'`, -1)
	// MatchByKey4B := "LNAME"
	// MatchByValue4B := strings.Replace(input.MatchKeys.LNAME.Value, "'", `\'`, -1)
	// MatchByKey4C := "FINITIAL"
	// MatchByValue4C := strings.Replace(input.MatchKeys.FINITIAL.Value, "'", `\'`, -1)
	// MatchByKey4D := "AD1NO"
	// MatchByValue4D := strings.Replace(input.MatchKeys.AD1NO.Value, "'", `\'`, -1)
	// MISSING address type

	MatchByKey5A := "CITY"
	MatchByValue5A := strings.Replace(input.MatchKeys.CITY.Value, "'", `\'`, -1)
	MatchByKey5B := "STATE"
	MatchByValue5B := strings.Replace(input.MatchKeys.STATE.Value, "'", `\'`, -1)
	MatchByKey5C := "LNAME"
	MatchByValue5C := strings.Replace(input.MatchKeys.LNAME.Value, "'", `\'`, -1)
	MatchByKey5D := "FINITIAL"
	MatchByValue5D := strings.Replace(input.MatchKeys.FINITIAL.Value, "'", `\'`, -1)
	MatchByKey5E := "AD1NO"
	MatchByValue5E := strings.Replace(input.MatchKeys.AD1NO.Value, "'", `\'`, -1)
	// missing address type

	QueryText := fmt.Sprintf("with fiberlist as (SELECT id, fibers FROM `%s.%s.%s` WHERE (exists (select 1 from UNNEST(signatures) s where s.RecordID = '%s')) ", ProjectID, DatasetID, SetTableName, MatchByValue0)
	if len(MatchByValue1) > 0 {
		QueryText += fmt.Sprintf("OR (exists (select 1 from UNNEST(matchKeys) m, UNNEST(m.values) u  where m.key = '%s' and u = '%s')) ", MatchByKey1, MatchByValue1)
	}
	if len(MatchByValue2) > 0 {
		QueryText += fmt.Sprintf("OR (exists (select 1 from UNNEST(matchKeys) m, UNNEST(m.values) u  where m.key = '%s' and u = '%s')) ", MatchByKey2, MatchByValue2)
	}
	if len(MatchByValue3A) > 0 && len(MatchByValue3B) > 0 {
		QueryText += fmt.Sprintf("OR (exists (select 1 from UNNEST(matchKeys) m, UNNEST(m.values) u  where m.key = '%s' and u = '%s') AND exists (select 1 from UNNEST(matchKeys) m, UNNEST(m.values) u  where m.key = '%s' and u = '%s')) ", MatchByKey3A, MatchByValue3A, MatchByKey3B, MatchByValue3B)
	}
	if len(MatchByValue5A) > 0 && len(MatchByValue5B) > 0 && len(MatchByValue5C) > 0 && len(MatchByValue5D) > 0 && len(MatchByValue5E) > 0 {
		QueryText += fmt.Sprintf("OR (exists (select 1 from UNNEST(matchKeys) m, UNNEST(m.values) u  where m.key = '%s' and u = '%s') AND exists (select 1 from UNNEST(matchKeys) m, UNNEST(m.values) u  where m.key = '%s' and u = '%s') AND exists (select 1 from UNNEST(matchKeys) m, UNNEST(m.values) u  where m.key = '%s' and u = '%s') AND exists (select 1 from UNNEST(matchKeys) m, UNNEST(m.values) u  where m.key = '%s' and u = '%s') AND exists (select 1 from UNNEST(matchKeys) m, UNNEST(m.values) u  where m.key = '%s' and u = '%s')) ", MatchByKey5A, MatchByValue5A, MatchByKey5B, MatchByValue5B, MatchByKey5C, MatchByValue5C, MatchByKey5D, MatchByValue5D, MatchByKey5E, MatchByValue5E)
	}
	QueryText += ") select distinct id, f from fiberlist, unnest(fibers) f"
	log.Printf("Match Query Text: %s", QueryText)
	BQQuery := bq.Query(QueryText)
	BQQuery.Location = "US"
	BQJob, err := BQQuery.Run(ctx)
	if err != nil {
		log.Fatalf("%v Could not query bq: %v", input.Signature.EventID, err)
		return err
	}
	BQStatus, err := BQJob.Wait(ctx)
	if err != nil {
		log.Fatalf("%v Error while waiting for bq job: %v", input.Signature.EventID, err)
		return err
	}
	if err := BQStatus.Err(); err != nil {
		log.Fatalf("%v bq execution error: %v", input.Signature.EventID, err)
		return err
	}
	BQIterator, err := BQJob.Read(ctx)

	// Collect all fiber IDs
	var FiberCollection []string
	var ExpiredSetCollection []string
	for {
		var fibers []bigquery.Value
		err = BQIterator.Next(&fibers)
		if err == iterator.Done {
			break
		} else if err != nil {
			log.Fatalf("%v bq exception getting fiber: %v", input.Signature.EventID, err)
		} else {
			log.Printf("Fetched from BQ: %v", fibers)
			// with the query above, we get 2 elements, first value is set id, second is fiber id
			// collect the set id for deletion
			// collect the fiber ids to build new set
			for i, f := range fibers {
				if i == 0 {
					fs := fmt.Sprintf("%v", f)
					if !Contains(ExpiredSetCollection, fs) {
						ExpiredSetCollection = append(ExpiredSetCollection, fs)
					}
				} else {
					fs := fmt.Sprintf("%v", f)
					if !Contains(FiberCollection, fs) {
						FiberCollection = append(FiberCollection, fs)
					}
				}
			}
		}
	}

	log.Printf("Fiber Collection: %v", FiberCollection)

	// get all the Fibers

	var FiberKeys []*datastore.Key
	var Fibers []PeopleFiberDS
	for _, fiber := range FiberCollection {
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

	log.Printf("Fibers: %v", Fibers)

	var output People360Output
	var FiberSignatures []Signature
	output.ID = uuid.New().String()
	MatchKeyList := structs.Names(&PeopleOutput{})
	FiberMatchKeys := make(map[string][]string)
	// collect all fiber match key values
	for _, name := range MatchKeyList {
		FiberMatchKeys[name] = []string{}
	}
	var SetMembers []PeopleSetMember
	for i, fiber := range Fibers {
		log.Printf("loaded fiber %v of %v: %v", i, len(Fibers), fiber)
		FiberSignatures = append(FiberSignatures, Signature{
			OwnerID:   fiber.OwnerID,
			Source:    fiber.Source,
			EventType: fiber.EventType,
			EventID:   fiber.EventID,
			RecordID:  fiber.RecordID,
		})
		SetMembers = append(SetMembers, PeopleSetMember{
			SetID:     output.ID,
			OwnerID:   fiber.OwnerID,
			Source:    fiber.Source,
			EventType: fiber.EventType,
			EventID:   fiber.EventID,
			RecordID:  fiber.RecordID,
			FiberID:   fiber.FiberID.Name,
		})
		for _, name := range MatchKeyList {
			value := strings.TrimSpace(GetMkField(&fiber.MatchKeys, name).Value)
			if len(value) > 0 && !Contains(FiberMatchKeys[name], value) {
				FiberMatchKeys[name] = append(FiberMatchKeys[name], value)
			}
		}
		log.Printf("FiberMatchKey values %v", FiberMatchKeys)
	}
	var MatchKeysFromFiber []MatchKey360
	for _, name := range MatchKeyList {
		mk360 := MatchKey360{
			Key:    name,
			Values: FiberMatchKeys[name],
		}
		MatchKeysFromFiber = append(MatchKeysFromFiber, mk360)
		log.Printf("mk.Values %v: %v", name, FiberMatchKeys[name])
	}

	output.MatchKeys = MatchKeysFromFiber

	HasNewValues := false
	// check to see if there are any new values
	for _, name := range MatchKeyList {
		mk := GetMatchKeyFields(output.MatchKeys, name)
		mk.Value = GetMkField(&input.MatchKeys, name).Value
		if !Contains(mk.Values, mk.Value) {
			HasNewValues = true
			break
		}
	}

	// stop processing if no new values
	if !HasNewValues {
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
	output.Fibers = append(FiberCollection, fiber.FiberID)
	output.Passthroughs = OutputPassthrough
	//output.TrustedIDs = append(output.TrustedIDs, input.MatchKeys.CAMPAIGNID.Value)
	var OutputMatchKeys []MatchKey360
	for _, name := range MatchKeyList {
		mk := GetMatchKeyFields(output.MatchKeys, name)
		mk.Key = name
		mk.Value = strings.TrimSpace(GetMkField(&input.MatchKeys, name).Value)
		// if blank, assign it a value
		if len(mk.Value) == 0 && len(mk.Values) > 0 {
			mk.Value = mk.Values[0]
		}
		if len(mk.Value) > 0 && !Contains(mk.Values, mk.Value) {
			mk.Values = append(mk.Values, mk.Value)
		}
		OutputMatchKeys = append(OutputMatchKeys, *mk)
	}
	output.MatchKeys = OutputMatchKeys

	SetMembers = append(SetMembers, PeopleSetMember{
		SetID:     output.ID,
		OwnerID:   input.Signature.OwnerID,
		Source:    input.Signature.Source,
		EventType: input.Signature.EventType,
		EventID:   input.Signature.EventID,
		RecordID:  input.Signature.RecordID,
		FiberID:   fiber.FiberID,
	})

	// store the set
	SetInserter := SetTable.Inserter()
	if err := SetInserter.Put(ctx, output); err != nil {
		log.Fatalf("error insertinng into set table %v", err)
		return nil
	}

	// record the set id in DS
	var setDS People360OutputDS
	setKey := datastore.NameKey(DSKindSet, output.ID, nil)
	setKey.Namespace = dsNameSpace
	setDS.ID = output.ID
	setDS.Fibers = output.Fibers
	setDS.CreatedAt = output.CreatedAt
	PopulateSetOutputSignatures(&setDS, output.Signatures)
	PopulateSetOutputMatchKeys(&setDS, output.MatchKeys)
	if _, err := ds.Put(ctx, setKey, &setDS); err != nil {
		log.Fatalf("Exception storing set with sig %v, error %v", input.Signature, err)
	}

	var goldenDS People360GoldenDS
	goldenKey := datastore.NameKey(DSKindGolden, output.ID, nil)
	goldenKey.Namespace = dsNameSpace
	goldenDS.ID = output.ID
	goldenDS.CreatedAt = output.CreatedAt
	PopulateGoldenOutputMatchKeys(&goldenDS, output.MatchKeys)
	if _, err := ds.Put(ctx, goldenKey, &goldenDS); err != nil {
		log.Fatalf("Exception storing golden record with sig %v, error %v", input.Signature, err)
	}

	// remove expired sets and setmembers from DS
	var SetKeys []*datastore.Key
	var MemberKeys []*datastore.Key
	var MemberDeleteKeys []*datastore.Key
	for _, set := range ExpiredSetCollection {
		setKey := datastore.NameKey(DSKindSet, set, nil)
		setKey.Namespace = dsNameSpace
		SetKeys = append(SetKeys, setKey)
		query := datastore.NewQuery(DSKindMember).Namespace(dsNameSpace).Filter("SetID =", set).KeysOnly()
		if _, err := ds.GetAll(ctx, query, &MemberDeleteKeys); err != nil {
			log.Fatalf("Error querying setmembers by set id: %v", err)
		} else {
			for _, s := range MemberDeleteKeys {
				MemberKeys = append(MemberKeys, s)
			}
		}
	}
	if err := ds.DeleteMulti(ctx, SetKeys); err != nil {
		log.Fatalf("Error deleting sets: %v", err)
	}
	if err := ds.DeleteMulti(ctx, MemberKeys); err != nil {
		log.Fatalf("Error deleting setmemberss: %v", err)
	}

	// get all set members to delete

	// 	var FoundFibers []PeopleFiber
	// 	query := datastore.NewQuery(dsKind).Namespace(dsKey.Namespace).Filter("FiberID =", fiber).Limit(1)
	// 	if _, err := ds.GetAll(ctx, query, &FoundFibers); err != nil {
	// 		log.Fatalf("Error querying fiber: %v", err)
	// 	}

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
		log.Fatalf("%v Could not pub to pubsub: %v", input.Signature.EventID, err)
	} else {
		log.Printf("%v pubbed record as message id %v: %v", input.Signature.EventID, psid, string(outputJSON))
	}

	topic2.Publish(ctx, &pubsub.Message{
		Data: outputJSON,
		Attributes: map[string]string{
			"type":   "people",
			"source": "360",
		},
	})

	// store set member in DS
	var SetMemberKeys []*datastore.Key
	for i := 0; i < len(SetMembers); i++ {
		memberKey := datastore.IncompleteKey(DSKindMember, nil)
		memberKey.Namespace = dsNameSpace
		SetMemberKeys = append(SetMemberKeys, memberKey)
	}

	if _, err := ds.PutMulti(ctx, SetMemberKeys, SetMembers); err != nil {
		log.Fatalf("Exception storing Set Members sig %v, error %v", input.Signature, err)
	}
	return nil
}

func GetMkField(v *PeopleOutput, field string) MatchKeyField {
	r := reflect.ValueOf(v)
	f := reflect.Indirect(r).FieldByName(field)
	return f.Interface().(MatchKeyField)
}

func GetMatchKeyFields(v []MatchKey360, key string) *MatchKey360 {
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

func PersistInES(ctx context.Context, v interface{}) bool {
	esJSON, _ := json.Marshal(v)
	esID := uuid.New().String()
	esReq := esapi.IndexRequest{
		Index:        ESIndex,
		DocumentType: "record",
		DocumentID:   esID,
		Body:         bytes.NewReader(esJSON),
		Refresh:      "true",
	}
	esRes, err := esReq.Do(ctx, es)
	if err != nil {
		log.Fatalf("Error getting response: %s", err)
	}
	defer esRes.Body.Close()

	if esRes.IsError() {
		resB, _ := ioutil.ReadAll(esRes.Body)
		log.Printf("[%s] Error indexing document ID=%v, Message=%v", esRes.Status(), esID, string(resB))
		return false
	} else {
		resB, _ := ioutil.ReadAll(esRes.Body)
		log.Printf("[%s] document ID=%v, Message=%v", esRes.Status(), esID, string(resB))
		return true
	}
}

func GetFiberDS(v *PeopleFiber) PeopleFiberDS {
	p := PeopleFiberDS{
		OwnerID:     v.Signature.OwnerID,
		Source:      v.Signature.Source,
		EventType:   v.Signature.EventType,
		EventID:     v.Signature.EventID,
		RecordID:    v.Signature.RecordID,
		Passthrough: v.Passthrough,
		MatchKeys:   v.MatchKeys,
		CreatedAt:   v.CreatedAt,
	}
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

func SetPeople360SetOutputFieldValues(v *People360OutputDS, field string, value []string) {
	r := reflect.ValueOf(v)
	f := reflect.Indirect(r).FieldByName(field)
	f.Set(reflect.ValueOf(value))
	LogDev(fmt.Sprintf("SetOutputFieldValues: %v %v", field, value))
}

func SetPeople360GoldenOutputFieldValue(v *People360GoldenDS, field string, value string) {
	r := reflect.ValueOf(v)
	f := reflect.Indirect(r).FieldByName(field)
	f.Set(reflect.ValueOf(value))
	LogDev(fmt.Sprintf("SetOutputFieldValue: %v %v", field, value))
}

func PopulateSetOutputSignatures(target *People360OutputDS, values []Signature) {
	KeyList := structs.Names(&Signature{})
	for _, key := range KeyList {
		SetPeople360SetOutputFieldValues(target, key, GetSignatureSliceValues(values, key))
	}
}

func PopulateSetOutputMatchKeys(target *People360OutputDS, values []MatchKey360) {
	KeyList := structs.Names(&PeopleOutput{})
	for _, key := range KeyList {
		SetPeople360SetOutputFieldValues(target, key, GetSetValuesFromMatchKeys(values, key))
	}
}

func PopulateGoldenOutputMatchKeys(target *People360GoldenDS, values []MatchKey360) {
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

func LogDev(s string) {
	if dev {
		log.Printf(s)
	}
}
