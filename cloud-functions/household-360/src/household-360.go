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
	OwnerID   int64  `json:"ownerId"`
	Source    string `json:"source"`
	EventID   string `json:"eventId"`
	EventType string `json:"eventType"`
	RecordID  string `json:"recordId"`
}

type PeopleInput struct {
	Signature   Signature         `json:"signature"`
	Passthrough map[string]string `json:"passthrough"`
	MatchKeys   PeopleOutput      `json:"matchkeys`
}

type HouseHoldFiber struct {
	Signature   Signature         `json:"signature" bigquery:"signature"`
	Passthrough map[string]string `json:"passthrough" bigquery:"passthrough"`
	MatchKeys   PeopleOutput      `json:"matchkeys bigquery:"matchkeys"`
	FiberID     string            `json:"fiberId" bigquery:"id"`
	CreatedAt   time.Time         `json:"createdAt" bigquery:"createdAt"`
}

type MatchKeyField struct {
	Value  string `json:"value"`
	Source string `json:"source"`
	Type   string `json:"type"`
}

type PeopleOutput struct {
	FNAME    MatchKeyField `json:"fname"`
	FINITIAL MatchKeyField `json:"finitial"`
	LNAME    MatchKeyField `json:"lname"`
	CITY     MatchKeyField `json:"city"`
	STATE    MatchKeyField `json:"state"`
	ZIP      MatchKeyField `json:"zip"`
	ZIP5     MatchKeyField `json:"zip5"`
	COUNTRY  MatchKeyField `json:"country"`
	EMAIL    MatchKeyField `json:"email"`
	PHONE    MatchKeyField `json:"phone"`
	AD1      MatchKeyField `json:"ad1"`
	AD2      MatchKeyField `json:"ad2"`
	ADTYPE   MatchKeyField `json:"adType"`

	TRUSTEDID MatchKeyField `json:"trustedId"`

	CLIENTID   MatchKeyField `json:"clientId"`
	SALUTATION MatchKeyField `json:"salutation"`
	NICKNAME   MatchKeyField `json:"nickname"`

	GENDER MatchKeyField `json:"gender"`
	AGE    MatchKeyField `json:"age"`
	DOB    MatchKeyField `json:"dob"`

	MAILROUTE MatchKeyField `json:"mailRoute"`

	ORGANIZATION MatchKeyField `json:"organization"`
	TITLE        MatchKeyField `json:"title"`
	ROLE         MatchKeyField `json:"role"`
	STATUS       MatchKeyField `json:"status"`
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
var SetTableName = os.Getenv("SETTABLE")
var FiberTableName = os.Getenv("FIBERTABLE")

var ps *pubsub.Client
var topic *pubsub.Topic

var bq bigquery.Client
var bs bigquery.Schema
var bc bigquery.Schema

// var setSchema bigquery.Schema

func init() {
	ctx := context.Background()
	ps, _ = pubsub.NewClient(ctx, ProjectID)
	topic = ps.Topic(PubSubTopic)
	bq, _ := bigquery.NewClient(ctx, ProjectID)
	bs, _ := bigquery.InferSchema(HouseHold360Output{})
	bc, _ := bigquery.InferSchema(HouseHoldFiber{})

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
			Value:  input.MatchKeys.FNAME.Value[0:0],
			Source: input.MatchKeys.FNAME.Source,
		}
	}
	if len(input.MatchKeys.ZIP.Value) > 0 {
		input.MatchKeys.ZIP5 = MatchKeyField{
			Value:  input.MatchKeys.ZIP.Value[0:4],
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
	DatasetID := strconv.FormatInt(input.Signature.OwnerID, 10)
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

	// store the fiber
	var fiber HouseHoldFiber
	fiber.CreatedAt = time.Now()
	fiber.FiberID = uuid.New().String()
	fiber.MatchKeys = input.MatchKeys
	fiber.Passthrough = input.Passthrough
	fiber.Signature = input.Signature

	FiberInserter := SetTable.Inserter()
	if err := FiberInserter.Put(ctx, fiber); err != nil {
		log.Fatalf("error insertinng into fiber table %v", err)
		return nil
	}

	// locate existing set
	MatchByKey4A := "ZIP5"
	MatchByValue4A := strings.Replace(input.MatchKeys.ZIP5.Value, "'", "\\'", -1)
	MatchByKey4B := "LNAME"
	MatchByValue4B := strings.Replace(input.MatchKeys.LNAME.Value, "'", "\\'", -1)
	// MISSING STREET NUMBER

	MatchByKey5A := "CITY"
	MatchByValue5A := strings.Replace(input.MatchKeys.CITY.Value, "'", "\\'", -1)
	MatchByKey5B := "STATE"
	MatchByValue5B := strings.Replace(input.MatchKeys.STATE.Value, "'", "\\'", -1)
	MatchByKey5C := "LNAME"
	MatchByValue5C := strings.Replace(input.MatchKeys.LNAME.Value, "'", "\\'", -1)
	// MISSING STREET NUMBER

	MatchByKey6A := "AD2"
	MatchByValue6A := strings.Replace(input.MatchKeys.AD2.Value, "'", "\\'", -1)
	MatchByKey6B := "ZIP5"
	MatchByValue6B := strings.Replace(input.MatchKeys.ZIP5.Value, "'", "\\'", -1)
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
		var fibers []string
		err = BQIterator.Next(&fibers)
		if err == iterator.Done {
			break
		} else if err != nil {
			log.Fatalf("%v bq returned value not matching expected type: %v", input.Signature.EventID, err)
		} else {
			for _, f := range fibers {
				if !Contains(FiberCollection, f) {
					FiberCollection = append(FiberCollection, f)
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
	if len(input.Passthrough) > 0 {
		for mapKey, mapValue := range input.Passthrough {
			pt := Passthrough360{
				Name:  mapKey,
				Value: mapValue,
			}
			output.Passthroughs = append(output.Passthroughs, pt)
		}
	}
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
	})
	psid, err := psresult.Get(ctx)
	_, err = psresult.Get(ctx)
	if err != nil {
		log.Fatalf("%v Could not pub to pubsub: %v", input.Signature.EventID, err)
	} else {
		log.Printf("%v pubbed record as message id %v: %v", input.Signature.EventID, psid, string(outputJSON))
	}

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
