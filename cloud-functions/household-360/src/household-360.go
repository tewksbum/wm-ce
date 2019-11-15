package household360

import (
	"context"
	"encoding/json"
	"fmt"
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

type HouseHoldFiber struct {
	Signature Signature `json:"signature" bigquery:"signature"`
	//Passthrough map[string]string `json:"passthrough" bigquery:"passthrough"`
	Passthrough []Passthrough360 `json:"passthrough" bigquery:"passthrough"`
	MatchKeys   HouseHoldOutput  `json:"matchkeys" bigquery:"matchkeys"`
	FiberID     string           `json:"fiberId" bigquery:"id"`
	CreatedAt   time.Time        `json:"createdAt" bigquery:"createdAt"`
}

type MatchKeyField struct {
	Value  string `json:"value"`
	Source string `json:"source"`
	Type   string `json:"type"`
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

type HouseHoldOutput struct {
	LNAME   MatchKeyField `json:"lname" bigquery:"lname"`
	CITY    MatchKeyField `json:"city" bigquery:"city"`
	STATE   MatchKeyField `json:"state" bigquery:"state"`
	ZIP     MatchKeyField `json:"zip" bigquery:"zip"`
	ZIP5    MatchKeyField `json:"zip5" bigquery:"zip5"`
	COUNTRY MatchKeyField `json:"country" bigquery:"country"`
	AD1     MatchKeyField `json:"ad1" bigquery:"ad1"`
	AD2     MatchKeyField `json:"ad2" bigquery:"ad2"`
	ADTYPE  MatchKeyField `json:"adType" bigquery:"adtype"`
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

type HouseHold360Output struct {
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

var ps *pubsub.Client
var topic *pubsub.Topic
var topic2 *pubsub.Topic

var bq *bigquery.Client
var bs bigquery.Schema
var bc bigquery.Schema

// var setSchema bigquery.Schema

func init() {
	ctx := context.Background()
	ps, _ = pubsub.NewClient(ctx, ProjectID)
	topic = ps.Topic(PubSubTopic)
	topic2 = ps.Topic(PubSubTopic2)
	bq, _ = bigquery.NewClient(ctx, ProjectID)
	bs, _ = bigquery.InferSchema(HouseHold360Output{})
	bc, _ = bigquery.InferSchema(HouseHoldFiber{})

	log.Printf("init completed, pubsub topic name: %v, bq client: %v, bq schema: %v, %v", topic, bq, bs, bc)
}

func HouseHold360(ctx context.Context, m PubSubMessage) error {
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
	}
	FiberTable := bq.Dataset(DatasetID).Table(FiberTableName)
	if err := FiberTable.Create(ctx, fiberMeta); err != nil {
	}

	// map the matchkeys from people to household
	var HouseholdMatchKeys HouseHoldOutput
	HouseholdMatchKeys.LNAME = input.MatchKeys.LNAME
	HouseholdMatchKeys.CITY = input.MatchKeys.CITY
	HouseholdMatchKeys.STATE = input.MatchKeys.STATE
	HouseholdMatchKeys.ZIP = input.MatchKeys.ZIP
	HouseholdMatchKeys.ZIP5 = input.MatchKeys.ZIP5
	HouseholdMatchKeys.COUNTRY = input.MatchKeys.COUNTRY
	HouseholdMatchKeys.AD1 = input.MatchKeys.AD1
	HouseholdMatchKeys.AD2 = input.MatchKeys.AD2
	HouseholdMatchKeys.ADTYPE = input.MatchKeys.ADTYPE

	// store the fiber
	OutputPassthrough := ConvertPassthrough(input.Passthrough)
	var fiber HouseHoldFiber
	fiber.CreatedAt = time.Now()
	fiber.FiberID = uuid.New().String()
	fiber.MatchKeys = HouseholdMatchKeys
	fiber.Passthrough = OutputPassthrough
	fiber.Signature = input.Signature

	FiberInserter := FiberTable.Inserter()
	if err := FiberInserter.Put(ctx, fiber); err != nil {
		log.Fatalf("error insertinng into fiber table %v", err)
		return nil
	}

	// locate existing set
	MatchByKey4A := "ZIP5"
	MatchByValue4A := strings.Replace(HouseholdMatchKeys.ZIP5.Value, "'", "\\'", -1)
	MatchByKey4B := "LNAME"
	MatchByValue4B := strings.Replace(HouseholdMatchKeys.LNAME.Value, "'", "\\'", -1)
	// MISSING STREET NUMBER

	MatchByKey5A := "CITY"
	MatchByValue5A := strings.Replace(HouseholdMatchKeys.CITY.Value, "'", "\\'", -1)
	MatchByKey5B := "STATE"
	MatchByValue5B := strings.Replace(HouseholdMatchKeys.STATE.Value, "'", "\\'", -1)
	MatchByKey5C := "LNAME"
	MatchByValue5C := strings.Replace(HouseholdMatchKeys.LNAME.Value, "'", "\\'", -1)
	// MISSING STREET NUMBER

	MatchByKey6A := "AD2"
	MatchByValue6A := strings.Replace(HouseholdMatchKeys.AD2.Value, "'", "\\'", -1)
	MatchByKey6B := "ZIP5"
	MatchByValue6B := strings.Replace(HouseholdMatchKeys.ZIP5.Value, "'", "\\'", -1)
	// MISSING STREET NUMBER

	QueryText := fmt.Sprintf("SELECT fibers FROM `%s.%s.%s`, UNNEST(matchKeys) m, UNNEST(m.values)u WHERE "+
		"(m.key = '%s' and u = '%s' AND m.key = '%s' and u = '%s') OR "+
		"(m.key = '%s' and u = '%s' AND m.key = '%s' and u = '%s' AND m.key = '%s' and u = '%s') OR "+
		"(m.key = '%s' and u = '%s' AND m.key = '%s' and u = '%s')"+
		"ORDER BY timestamp DESC", ProjectID, DatasetID, SetTableName,
		MatchByKey4A, MatchByValue4A, MatchByKey4B, MatchByValue4B,
		MatchByKey5A, MatchByValue5A, MatchByKey5B, MatchByValue5B, MatchByKey5C, MatchByValue5C,
		MatchByKey6A, MatchByValue6A, MatchByKey6B, MatchByValue6B)

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

	// get all the Fibers
	var Fibers []HouseHoldFiber
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
			var fiber HouseHoldFiber
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

	var output HouseHold360Output
	var FiberMatchKeys []MatchKey360

	MatchKeyList := structs.Names(&HouseHold360Output{})
	HasNewValues := false
	// check to see if there are any new values
	for _, name := range MatchKeyList {
		log.Printf("Getting %v value", name)
		mk := GetMatchKeyFields(output.MatchKeys, name)
		mk.Value = GetMkField(&HouseholdMatchKeys, name).Value
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
		mk.Value = GetMkField(&HouseholdMatchKeys, name).Value
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
			"type":   "household",
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
			"type":   "household",
			"source": "360",
		},
	})

	return nil
}

func GetMkField(v *HouseHoldOutput, field string) MatchKeyField {
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
