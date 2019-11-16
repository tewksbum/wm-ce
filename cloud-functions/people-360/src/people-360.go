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
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
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
	OwnerID   int64  `json:"ownerId" bigquery:"ownerid"`
	Source    string `json:"source" bigquery:"source"`
	EventID   string `json:"eventId" bigquery:"eventid"`
	EventType string `json:"eventType" bigquery:"eventtype"`
	RecordID  string `json:"recordId" bigquery:"recordid"`
}

type PeopleInput struct {
	Signature   Signature         `json:"signature"`
	Passthrough map[string]string `json:"passthrough"`
	MatchKeys   PeopleOutput      `json:"matchkeys`
}

type PeopleFiber struct {
	Signature Signature `json:"signature" bigquery:"signature"`
	// Passthrough map[string]string `json:"passthrough" bigquery:"passthrough"`
	Passthrough []Passthrough360 `json:"passthrough" bigquery:"passthrough"`
	MatchKeys   PeopleOutput     `json:"matchkeys" bigquery:"matchkeys"`
	FiberID     string           `json:"fiberId" bigquery:"id"`
	CreatedAt   time.Time        `json:"createdAt" bigquery:"createdAt"`
}

type MatchKeyField struct {
	Value  string `json:"value" bigquery:"value"`
	Source string `json:"source" bigquery:"source"`
	Type   string `json:"type" bigquery:"type"`
}

type PeopleOutput struct {
	FNAME    MatchKeyField `json:"fname" bigquery:"fname"`
	FINITIAL MatchKeyField `json:"finitial" bigquery:"finitial"`
	LNAME    MatchKeyField `json:"lname" bigquery:"lname"`
	CITY     MatchKeyField `json:"city" bigquery:"city"`
	STATE    MatchKeyField `json:"state" bigquery:"state"`
	ZIP      MatchKeyField `json:"zip" bigquery:"zip"`
	ZIP5     MatchKeyField `json:"zip5" bigquery:"zip5"`
	COUNTRY  MatchKeyField `json:"country" bigquery:"country"`
	EMAIL    MatchKeyField `json:"email" bigquery:"email"`
	PHONE    MatchKeyField `json:"phone" bigquery:"phone"`
	AD1      MatchKeyField `json:"ad1" bigquery:"ad1"`
	AD2      MatchKeyField `json:"ad2" bigquery:"ad2"`
	ADTYPE   MatchKeyField `json:"adType" bigquery:"adtype"`

	TRUSTEDID MatchKeyField `json:"trustedId" bigquery:"trustedid"`

	CLIENTID   MatchKeyField `json:"clientId" bigquery:"clientid"`
	SALUTATION MatchKeyField `json:"salutation" bigquery:"salutation"`
	NICKNAME   MatchKeyField `json:"nickname" bigquery:"nickname"`

	GENDER MatchKeyField `json:"gender" bigquery:"gender"`
	AGE    MatchKeyField `json:"age" bigquery:"age"`
	DOB    MatchKeyField `json:"dob" bigquery:"dob"`

	MAILROUTE MatchKeyField `json:"mailRoute" bigquery:"mailroute"`

	ORGANIZATION MatchKeyField `json:"organization" bigquery:"organization"`
	TITLE        MatchKeyField `json:"title" bigquery:"title"`
	ROLE         MatchKeyField `json:"role" bigquery:"role"`
	STATUS       MatchKeyField `json:"status" bigquery:"status"`
}

type Signature360 struct {
	OwnerID   int64  `json:"ownerId" bigquery:"ownerId"`
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
	TimeStamp    time.Time        `json:"timestamp" bigquery:"timestamp"`
	Fibers       []string         `json:"fibers" bigquery:"fibers"`
	Passthroughs []Passthrough360 `json:"passthroughs" bigquery:"passthroughs"`
	MatchKeys    []MatchKey360    `json:"matchKeys" bigquery:"matchKeys"`
}

var ProjectID = os.Getenv("PROJECTID")
var PubSubTopic = os.Getenv("PSOUTPUT")
var PubSubTopic2 = os.Getenv("PSOUTPUT2")
var BQPrefix = os.Getenv("BQPREFIX")
var SetTableName = os.Getenv("SETTABLE")
var FiberTableName = os.Getenv("FIBERTABLE")
var ESUrl = os.Getenv("ELASTICURL")
var ESUid = os.Getenv("ELASTICUSER")
var ESPwd = os.Getenv("ELASTICPWD")
var ESIndex = os.Getenv("ELASTICINDEX")

var ps *pubsub.Client
var topic *pubsub.Topic
var topic2 *pubsub.Topic

var bq *bigquery.Client
var bs bigquery.Schema
var bc bigquery.Schema
var es *elasticsearch.Client

// var setSchema bigquery.Schema

func init() {
	ctx := context.Background()
	ps, _ = pubsub.NewClient(ctx, ProjectID)
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
	DatasetID := BQPrefix + strconv.FormatInt(input.Signature.OwnerID, 10)
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

	FiberInserter := FiberTable.Inserter()
	if err := FiberInserter.Put(ctx, fiber); err != nil {
		log.Fatalf("error insertinng into fiber table %v", err)
		return nil
	}

	// locate existing set
	MatchByValue0 := input.Signature.RecordID

	MatchByKey1 := "TRUSTEDID"
	MatchByValue1 := strings.Replace(input.MatchKeys.TRUSTEDID.Value, "'", "\\'", -1)

	MatchByKey2 := "EMAIL"
	MatchByValue2 := strings.Replace(input.MatchKeys.EMAIL.Value, "'", "\\'", -1)

	MatchByKey3A := "PHONE"
	MatchByValue3A := strings.Replace(input.MatchKeys.PHONE.Value, "'", "\\'", -1)
	MatchByKey3B := "FINITIAL"
	MatchByValue3B := strings.Replace(input.MatchKeys.FINITIAL.Value, "'", "\\'", -1)

	MatchByKey4A := "ZIP5"
	MatchByValue4A := strings.Replace(input.MatchKeys.ZIP5.Value, "'", "\\'", -1)
	MatchByKey4B := "LNAME"
	MatchByValue4B := strings.Replace(input.MatchKeys.LNAME.Value, "'", "\\'", -1)
	MatchByKey4C := "FINITIAL"
	MatchByValue4C := strings.Replace(input.MatchKeys.FINITIAL.Value, "'", "\\'", -1)
	// MISSING STREET NUMBER
	// MISSING address type

	MatchByKey5A := "CITY"
	MatchByValue5A := strings.Replace(input.MatchKeys.CITY.Value, "'", "\\'", -1)
	MatchByKey5B := "STATE"
	MatchByValue5B := strings.Replace(input.MatchKeys.STATE.Value, "'", "\\'", -1)
	MatchByKey5C := "LNAME"
	MatchByValue5C := strings.Replace(input.MatchKeys.LNAME.Value, "'", "\\'", -1)
	MatchByKey5D := "FINITIAL"
	MatchByValue5D := strings.Replace(input.MatchKeys.FINITIAL.Value, "'", "\\'", -1)
	// MISSING STREET NUMBER
	// missing address type

	MatchByKey6A := "ORGANIZATION"
	MatchByValue6A := strings.Replace(input.MatchKeys.ORGANIZATION.Value, "'", "\\'", -1)
	MatchByKey6B := "LNAME"
	MatchByValue6B := strings.Replace(input.MatchKeys.LNAME.Value, "'", "\\'", -1)
	MatchByKey6C := "FNAME"
	MatchByValue6C := strings.Replace(input.MatchKeys.FNAME.Value, "'", "\\'", -1)
	MatchByKey6D := "TITLE"
	MatchByValue6D := strings.Replace(input.MatchKeys.TITLE.Value, "'", "\\'", -1)

	QueryText := fmt.Sprintf("SELECT fibers FROM `%s.%s.%s`, UNNEST(matchKeys) m, UNNEST(m.values)u, UNNEST(signatures) s WHERE "+
		"(s.RecordID = '%s') OR "+
		"(m.key = '%s' and u = '%s') OR "+
		"(m.key = '%s' and u = '%s') OR "+
		"(m.key = '%s' and u = '%s' AND m.key = '%s' and u = '%s') OR "+
		"(m.key = '%s' and u = '%s' AND m.key = '%s' and u = '%s' AND m.key = '%s' and u = '%s') OR "+
		"(m.key = '%s' and u = '%s' AND m.key = '%s' and u = '%s' AND m.key = '%s' and u = '%s' AND m.key = '%s' and u = '%s') OR "+
		"(m.key = '%s' and u = '%s' AND m.key = '%s' and u = '%s' AND m.key = '%s' and u = '%s' AND m.key = '%s' and u = '%s')"+
		"ORDER BY timestamp DESC", ProjectID, DatasetID, SetTableName,
		MatchByValue0,
		MatchByKey1, MatchByValue1,
		MatchByKey2, MatchByValue2,
		MatchByKey3A, MatchByValue3A, MatchByKey3B, MatchByValue3B,
		MatchByKey4A, MatchByValue4A, MatchByKey4B, MatchByValue4B, MatchByKey4C, MatchByValue4C,
		MatchByKey5A, MatchByValue5A, MatchByKey5B, MatchByValue5B, MatchByKey5C, MatchByValue5C, MatchByKey5D, MatchByValue5D,
		MatchByKey6A, MatchByValue6A, MatchByKey6B, MatchByValue6B, MatchByKey6C, MatchByValue6C, MatchByKey6D, MatchByValue6D)
	log.Printf("Query Text: %s", QueryText)
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
	for {
		var fibers []bigquery.Value
		err = BQIterator.Next(&fibers)
		if err == iterator.Done {
			break
		} else if err != nil {
			log.Fatalf("%v bq exception getting fiber: %v", input.Signature.EventID, err)
		} else {
			for _, f := range fibers {
				fs := fmt.Sprintf("%s", f)
				if !Contains(FiberCollection, fs) {
					FiberCollection = append(FiberCollection, fs)
				}
			}
		}
	}

	log.Printf("Fiber Collection: %v", FiberCollection)

	// get all the Fibers
	var Fibers []PeopleFiber
	if len(FiberCollection) > 0 {
		FiberList := "'" + strings.Join(FiberCollection, "', '") + "'"
		QueryText := fmt.Sprintf("SELECT * FROM `%s.%s.%s` WHERE id IN (%s) ORDER BY createdAt DESC", ProjectID, DatasetID, FiberTableName, FiberList)
		BQQuery := bq.Query(QueryText)
		BQQuery.Location = "US"
		BQJob, err := BQQuery.Run(ctx)
		if err != nil {
			log.Fatalf("%v Could not query bq fibers: %v", input.Signature.EventID, err)
			return err
		}
		BQStatus, err := BQJob.Wait(ctx)
		if err != nil {
			log.Fatalf("%v Error while waiting for bq fibers job: %v", input.Signature.EventID, err)
			return err
		}
		if err := BQStatus.Err(); err != nil {
			log.Fatalf("%v bq fibers execution error: %v", input.Signature.EventID, err)
			return err
		}
		BQIterator, err := BQJob.Read(ctx)

		// Collect all fiber IDs
		for {
			var fiber PeopleFiber
			err = BQIterator.Next(&fiber)
			if err == iterator.Done {
				break
			} else if err != nil {
				log.Fatalf("%v bq returned value not matching expected type: %v", input.Signature.EventID, err)
			} else {
				Fibers = append(Fibers, fiber)
			}
		}
	}

	var output People360Output
	var FiberMatchKeys []MatchKey360

	MatchKeyList := structs.Names(&PeopleOutput{})
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

	// collect all fiber match key values
	for _, fiber := range Fibers {
		for _, name := range MatchKeyList {
			mk := GetMatchKeyFields(output.MatchKeys, name)
			value := GetMkField(&fiber.MatchKeys, name).Value
			if !Contains(mk.Values, value) && len(value) > 0 {
				mk.Values = append(mk.Values, value)
			}
		}

	}
	output.MatchKeys = FiberMatchKeys

	// append to the output value
	output.ID = uuid.New().String()
	output.TimeStamp = time.Now()
	output.Signatures = append(output.Signatures, input.Signature)
	output.Signature = Signature360{
		OwnerID:   input.Signature.OwnerID,
		Source:    input.Signature.Source,
		EventID:   input.Signature.EventID,
		EventType: input.Signature.EventType,
	}
	if output.CreatedAt.IsZero() {
		output.CreatedAt = time.Now()
	}
	output.Fibers = append(output.Fibers, input.Signature.RecordID)
	output.Passthroughs = OutputPassthrough
	//output.TrustedIDs = append(output.TrustedIDs, input.MatchKeys.CAMPAIGNID.Value)
	var OutputMatchKeys []MatchKey360
	for _, name := range MatchKeyList {
		mk := GetMatchKeyFields(output.MatchKeys, name)
		mk.Key = name
		mk.Value = GetMkField(&input.MatchKeys, name).Value
		if len(mk.Value) == 0 && len(mk.Values) > 0 {
			mk.Value = mk.Values[0]
		}
		if len(mk.Value) > 0 && !Contains(mk.Values, mk.Value) {
			mk.Values = append(mk.Values, mk.Value)
		}
		OutputMatchKeys = append(OutputMatchKeys, mk)
	}
	output.MatchKeys = OutputMatchKeys

	// store the set
	SetInserter := SetTable.Inserter()
	if err := SetInserter.Put(ctx, output); err != nil {
		log.Fatalf("error insertinng into set table %v", err)
		return nil
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

	return nil
}

func GetMkField(v *PeopleOutput, field string) MatchKeyField {
	r := reflect.ValueOf(v)
	f := reflect.Indirect(r).FieldByName(field)
	return f.Interface().(MatchKeyField)
}

func GetMatchKeyFields(v []MatchKey360, key string) MatchKey360 {
	for _, m := range v {
		if m.Key == key {
			return m
		}
	}
	return MatchKey360{}
}

func Contains(slice []string, item string) bool {
	set := make(map[string]struct{}, len(slice))
	for _, s := range slice {
		set[s] = struct{}{}
	}

	_, ok := set[item]
	return ok
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
