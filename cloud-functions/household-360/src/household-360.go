package household360

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

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/datastore"
	"cloud.google.com/go/pubsub"

	"github.com/fatih/structs"
	"github.com/google/uuid"
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
}

type HouseHoldFiber struct {
	Signature Signature `json:"signature" bigquery:"signature"`
	//Passthrough map[string]string `json:"passthrough" bigquery:"passthrough"`
	Passthrough []Passthrough360 `json:"passthrough" bigquery:"passthrough"`
	MatchKeys   HouseHoldOutput  `json:"matchkeys" bigquery:"matchkeys"`
	ID          string           `json:"fiberId" bigquery:"id"`
	CreatedAt   time.Time        `json:"createdAt" bigquery:"createdAt"`
}

type HouseHoldFiberDS struct {
	ID          *datastore.Key   `datastore:"__key__"`
	CreatedAt   time.Time        `datastore:"createdAt"`
	OwnerID     string           `datastore:"ownerid"`
	Source      string           `datastore:"source"`
	EventID     string           `datastore:"eventid"`
	EventType   string           `datastore:"eventtype"`
	RecordID    string           `datastore:"recordid"`
	LNAME       MatchKeyField    `datastore:"lname"`
	CITY        MatchKeyField    `datastore:"city"`
	STATE       MatchKeyField    `datastore:"state"`
	ZIP         MatchKeyField    `datastore:"zip"`
	ZIP5        MatchKeyField    `datastore:"zip5"`
	COUNTRY     MatchKeyField    `datastore:"country"`
	AD1         MatchKeyField    `datastore:"ad1"`
	AD1NO       MatchKeyField    `datastore:"ad1no"`
	AD2         MatchKeyField    `datastore:"ad2"`
	ADTYPE      MatchKeyField    `datastore:"adtype"`
	Passthrough []Passthrough360 `datastore:"passthrough"`
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

type HouseHoldOutput struct {
	LNAME   MatchKeyField `json:"lname" bigquery:"lname"`
	CITY    MatchKeyField `json:"city" bigquery:"city"`
	STATE   MatchKeyField `json:"state" bigquery:"state"`
	ZIP     MatchKeyField `json:"zip" bigquery:"zip"`
	ZIP5    MatchKeyField `json:"zip5" bigquery:"zip5"`
	COUNTRY MatchKeyField `json:"country" bigquery:"country"`
	AD1     MatchKeyField `json:"ad1" bigquery:"ad1"`
	AD1NO   MatchKeyField `json:"ad1no" bigquery:"ad1no"`
	AD2     MatchKeyField `json:"ad2" bigquery:"ad2"`
	ADTYPE  MatchKeyField `json:"adType" bigquery:"adtype"`
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

type HouseHoldSetDS struct {
	ID                 *datastore.Key `datastore:"__key__"`
	OwnerID            []string       `datastore:"ownerid"`
	Source             []string       `datastore:"source"`
	EventID            []string       `datastore:"eventid"`
	EventType          []string       `datastore:"eventtype"`
	RecordID           []string       `datastore:"recordid"`
	RecordIDNormalized []string       `datastore:"recordidnormalized"`
	CreatedAt          time.Time      `datastore:"createdat"`
	Fibers             []string       `datastore:"fibers"`
	LNAME              []string       `datastore:"lname"`
	LNAMENormalized    []string       `datastore:"lnamenormalized"`
	CITY               []string       `datastore:"city"`
	CITYNormalized     []string       `datastore:"citynormalized"`
	STATE              []string       `datastore:"state"`
	STATENormalized    []string       `datastore:"statenormalized"`
	ZIP                []string       `datastore:"zip"`
	ZIPNormalized      []string       `datastore:"zipnormalized"`
	ZIP5               []string       `datastore:"zip5"`
	ZIP5Normalized     []string       `datastore:"zip5normalized"`
	COUNTRY            []string       `datastore:"country"`
	COUNTRYNormalized  []string       `datastore:"countrynormalized"`
	AD1                []string       `datastore:"ad1"`
	AD1Normalized      []string       `datastore:"ad1normalized"`
	AD1NO              []string       `datastore:"ad1no"`
	AD1NONormalized    []string       `datastore:"ad1nonormalized"`
	AD2                []string       `datastore:"ad2"`
	AD2Normalized      []string       `datastore:"ad2normalized"`
	ADTYPE             []string       `datastore:"adtype"`
	ADTYPENormalized   []string       `datastore:"adtypenormalized"`
}

type HouseHoldGoldenDS struct {
	ID        *datastore.Key `datastore:"__key__"`
	CreatedAt time.Time      `datastore:"createdat"`
	LNAME     string         `datastore:"lname"`
	CITY      string         `datastore:"city"`
	STATE     string         `datastore:"state"`
	ZIP       string         `datastore:"zip"`
	ZIP5      string         `datastore:"zip5"`
	COUNTRY   string         `datastore:"country"`
	AD1       string         `datastore:"ad1"`
	AD1NO     string         `datastore:"ad1no"`
	AD2       string         `datastore:"ad2"`
	ADTYPE    string         `datastore:"adtype"`
}

var ProjectID = os.Getenv("PROJECTID")
var PubSubTopic = os.Getenv("PSOUTPUT")
var PubSubTopic2 = os.Getenv("PSOUTPUT2")
var BQPrefix = os.Getenv("BQPREFIX")
var SetTableName = os.Getenv("SETTABLE")
var FiberTableName = os.Getenv("FIBERTABLE")
var DSKindSet = os.Getenv("DSKINDSET")
var DSKindGolden = os.Getenv("DSKINDGOLDEN")
var DSKindFiber = os.Getenv("DSKINDFIBER")
var Env = os.Getenv("ENVIRONMENT")
var dev = Env == "dev"

var reAlphaNumeric = regexp.MustCompile("[^a-zA-Z0-9]+")

var ps *pubsub.Client
var topic *pubsub.Topic
var topic2 *pubsub.Topic

var bq *bigquery.Client
var bs bigquery.Schema
var bc bigquery.Schema
var ds *datastore.Client

// var setSchema bigquery.Schema

func init() {
	ctx := context.Background()
	ps, _ = pubsub.NewClient(ctx, ProjectID)
	topic = ps.Topic(PubSubTopic)
	topic2 = ps.Topic(PubSubTopic2)
	bq, _ = bigquery.NewClient(ctx, ProjectID)
	bs, _ = bigquery.InferSchema(HouseHold360Output{})
	bc, _ = bigquery.InferSchema(HouseHoldFiber{})
	ds, _ = datastore.NewClient(ctx, ProjectID)

	log.Printf("init completed, pubsub topic name: %v, bq client: %v, bq schema: %v, %v", topic, bq, bs, bc)
}

func HouseHold360(ctx context.Context, m PubSubMessage) error {
	var input PeopleInput
	if err := json.Unmarshal(m.Data, &input); err != nil {
		log.Fatalf("Unable to unmarshal message %v with error %v", string(m.Data), err)
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
	DatasetID := strings.ToLower(reAlphaNumeric.ReplaceAllString(BQPrefix+input.Signature.OwnerID, ""))
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
	HouseholdMatchKeys.AD1NO = input.MatchKeys.AD1NO
	HouseholdMatchKeys.AD2 = input.MatchKeys.AD2
	HouseholdMatchKeys.ADTYPE = input.MatchKeys.ADTYPE

	// store the fiber
	OutputPassthrough := ConvertPassthrough(input.Passthrough)
	var fiber HouseHoldFiber
	fiber.CreatedAt = time.Now()
	fiber.ID = uuid.New().String()
	fiber.MatchKeys = HouseholdMatchKeys
	fiber.Passthrough = OutputPassthrough
	fiber.Signature = input.Signature

	// store in BQ
	FiberInserter := FiberTable.Inserter()
	if err := FiberInserter.Put(ctx, fiber); err != nil {
		log.Fatalf("error insertinng into fiber table %v", err)
		return nil
	}

	// store in DS
	dsNameSpace := strings.ToLower(fmt.Sprintf("%v-%v", Env, input.Signature.OwnerID))
	dsKey := datastore.NameKey(DSKindFiber, fiber.ID, nil)
	dsKey.Namespace = dsNameSpace
	dsFiber := GetFiberDS(&fiber)
	dsFiber.ID = dsKey
	if _, err := ds.Put(ctx, dsKey, &dsFiber); err != nil {
		log.Fatalf("Exception storing Fiber sig %v, error %v", input.Signature, err)
	}

	// locate existing set
	MatchByKey4A := "ZIP5"
	MatchByValue4A := strings.Replace(input.MatchKeys.ZIP5.Value, "'", `''`, -1)
	MatchByKey4B := "LNAME"
	MatchByValue4B := strings.Replace(HouseholdMatchKeys.LNAME.Value, "'", `''`, -1)
	MatchByKey4C := "AD1NO"
	MatchByValue4C := strings.Replace(HouseholdMatchKeys.AD1NO.Value, "'", `''`, -1)

	MatchByKey5A := "CITY"
	MatchByValue5A := strings.Replace(HouseholdMatchKeys.CITY.Value, "'", `''`, -1)
	MatchByKey5B := "STATE"
	MatchByValue5B := strings.Replace(HouseholdMatchKeys.STATE.Value, "'", `''`, -1)
	MatchByKey5C := "LNAME"
	MatchByValue5C := strings.Replace(HouseholdMatchKeys.LNAME.Value, "'", `''`, -1)
	MatchByKey5D := "AD1NO"
	MatchByValue5D := strings.Replace(HouseholdMatchKeys.AD1NO.Value, "'", `''`, -1)

	MatchByKey6A := "AD2"
	MatchByValue6A := strings.Replace(HouseholdMatchKeys.AD2.Value, "'", `''`, -1)
	MatchByKey6B := "ZIP5"
	MatchByValue6B := strings.Replace(HouseholdMatchKeys.ZIP5.Value, "'", `''`, -1)
	MatchByKey6C := "AD1NO"
	MatchByValue6C := strings.Replace(HouseholdMatchKeys.AD1NO.Value, "'", `''`, -1)

	matchedSets := []HouseHoldSetDS{}
	queriedSets := []HouseHoldSetDS{}
	if len(MatchByValue4A) > 0 && len(MatchByValue4B) > 0 && len(MatchByValue4C) > 0 {
		setQuery := datastore.NewQuery(DSKindSet).Namespace(dsNameSpace).
			Filter(strings.ToLower(MatchByKey4A)+"normalized =", strings.ToUpper(MatchByValue4A)).
			Filter(strings.ToLower(MatchByKey4B)+"normalized =", strings.ToUpper(MatchByValue4B)).
			Filter(strings.ToLower(MatchByKey4C)+"normalized =", strings.ToUpper(MatchByValue4C))
		if _, err := ds.GetAll(ctx, setQuery, &queriedSets); err != nil {
			log.Fatalf("Error querying sets query 1: %v", err)
		} else {
			for _, s := range queriedSets {
				matchedSets = append(matchedSets, s)
			}
		}
	}
	if len(MatchByValue5A) > 0 && len(MatchByValue5B) > 0 && len(MatchByValue5C) > 0 && len(MatchByValue5D) > 0 {
		setQuery := datastore.NewQuery(DSKindSet).Namespace(dsNameSpace).
			Filter(strings.ToLower(MatchByKey5A)+"normalized =", strings.ToUpper(MatchByValue5A)).
			Filter(strings.ToLower(MatchByKey5B)+"normalized =", strings.ToUpper(MatchByValue5B)).
			Filter(strings.ToLower(MatchByKey5C)+"normalized =", strings.ToUpper(MatchByValue5C)).
			Filter(strings.ToLower(MatchByKey5D)+"normalized =", strings.ToUpper(MatchByValue5D))
		if _, err := ds.GetAll(ctx, setQuery, &queriedSets); err != nil {
			log.Fatalf("Error querying sets query 1: %v", err)
		} else {
			for _, s := range queriedSets {
				matchedSets = append(matchedSets, s)
			}
		}
	}
	if len(MatchByValue6A) > 0 && len(MatchByValue6B) > 0 && len(MatchByValue6C) > 0 {
		setQuery := datastore.NewQuery(DSKindSet).Namespace(dsNameSpace).
			Filter(strings.ToLower(MatchByKey6A)+"normalized =", strings.ToUpper(MatchByValue6A)).
			Filter(strings.ToLower(MatchByKey6B)+"normalized =", strings.ToUpper(MatchByValue6B)).
			Filter(strings.ToLower(MatchByKey6C)+"normalized =", strings.ToUpper(MatchByValue6C))
		if _, err := ds.GetAll(ctx, setQuery, &queriedSets); err != nil {
			log.Fatalf("Error querying sets query 1: %v", err)
		} else {
			for _, s := range queriedSets {
				matchedSets = append(matchedSets, s)
			}
		}
	}

	var matchedFibers []string
	var expiredSetCollection []string
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

	LogDev(fmt.Sprintf("Fiber Collection: %v", matchedFibers))

	// get all the Fibers
	var FiberKeys []*datastore.Key
	var Fibers []HouseHoldFiberDS
	for _, fiber := range matchedFibers {
		dsFiberGetKey := datastore.NameKey(DSKindFiber, fiber, nil)
		dsFiberGetKey.Namespace = dsNameSpace
		FiberKeys = append(FiberKeys, dsFiberGetKey)
		Fibers = append(Fibers, HouseHoldFiberDS{})
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

	var output HouseHold360Output
	var FiberSignatures []Signature
	output.ID = uuid.New().String()
	MatchKeyList := structs.Names(&HouseHoldOutput{})
	FiberMatchKeys := make(map[string][]string)
	// collect all fiber match key values
	for _, name := range MatchKeyList {
		FiberMatchKeys[name] = []string{}
	}
	//var SetMembers []HouseHoldSetMember
	for i, fiber := range Fibers {
		LogDev(fmt.Sprintf("loaded fiber %v of %v: %v", i, len(Fibers), fiber))
		FiberSignatures = append(FiberSignatures, Signature{
			OwnerID:   fiber.OwnerID,
			Source:    fiber.Source,
			EventType: fiber.EventType,
			EventID:   fiber.EventID,
			RecordID:  fiber.RecordID,
		})

		for _, name := range MatchKeyList {
			value := strings.TrimSpace(GetMatchKeyFieldFromDSFiber(&fiber, name).Value)
			if len(value) > 0 && !Contains(FiberMatchKeys[name], value) {
				FiberMatchKeys[name] = append(FiberMatchKeys[name], value)
			}
		}
		LogDev(fmt.Sprintf("FiberMatchKey values %v", FiberMatchKeys))
	}
	var MatchKeysFromFiber []MatchKey360
	for _, name := range MatchKeyList {
		mk360 := MatchKey360{
			Key:    name,
			Values: FiberMatchKeys[name],
		}
		MatchKeysFromFiber = append(MatchKeysFromFiber, mk360)
		LogDev(fmt.Sprintf("mk.Values %v: %v", name, FiberMatchKeys[name]))
	}

	output.MatchKeys = MatchKeysFromFiber

	HasNewValues := false
	// check to see if there are any new values
	for _, name := range MatchKeyList {
		mk := GetMatchKey360ByName(output.MatchKeys, name)
		mk.Value = GetMatchKeyFieldFromPeopleOutput(&input.MatchKeys, name).Value
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
	output.Fibers = append(matchedFibers, fiber.ID)
	output.Passthroughs = OutputPassthrough
	//output.TrustedIDs = append(output.TrustedIDs, input.MatchKeys.CAMPAIGNID.Value)
	var OutputMatchKeys []MatchKey360
	for _, name := range MatchKeyList {
		mk := GetMatchKey360ByName(output.MatchKeys, name)
		mk.Key = name
		mk.Value = strings.TrimSpace(GetMatchKeyFieldFromPeopleOutput(&input.MatchKeys, name).Value)
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

	// store the set
	SetInserter := SetTable.Inserter()
	if err := SetInserter.Put(ctx, output); err != nil {
		log.Fatalf("error insertinng into set table %v", err)
		return nil
	}

	// record the set id in DS
	var setDS HouseHoldSetDS
	setKey := datastore.NameKey(DSKindSet, output.ID, nil)
	setKey.Namespace = dsNameSpace
	setDS.ID = setKey
	setDS.Fibers = output.Fibers
	setDS.CreatedAt = output.CreatedAt
	PopulateSetOutputSignatures(&setDS, output.Signatures)
	PopulateSetOutputMatchKeys(&setDS, output.MatchKeys)
	if _, err := ds.Put(ctx, setKey, &setDS); err != nil {
		log.Fatalf("Exception storing set with sig %v, error %v", input.Signature, err)
	}

	var goldenDS HouseHoldGoldenDS
	goldenKey := datastore.NameKey(DSKindGolden, output.ID, nil)
	goldenKey.Namespace = dsNameSpace
	goldenDS.ID = goldenKey
	goldenDS.CreatedAt = output.CreatedAt
	PopulateGoldenOutputMatchKeys(&goldenDS, output.MatchKeys)
	if _, err := ds.Put(ctx, goldenKey, &goldenDS); err != nil {
		log.Fatalf("Exception storing golden record with sig %v, error %v", input.Signature, err)
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
	if err := ds.DeleteMulti(ctx, SetKeys); err != nil {
		log.Fatalf("Error deleting sets: %v", err)
	}
	if err := ds.DeleteMulti(ctx, GoldenKeys); err != nil {
		log.Fatalf("Error deleting golden records: %v", err)
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
		LogDev(fmt.Sprintf("%v pubbed record as message id %v: %v", input.Signature.EventID, psid, string(outputJSON)))
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

func GetFiberDS(v *HouseHoldFiber) HouseHoldFiberDS {
	p := HouseHoldFiberDS{
		OwnerID:     v.Signature.OwnerID,
		Source:      v.Signature.Source,
		EventType:   v.Signature.EventType,
		EventID:     v.Signature.EventID,
		RecordID:    v.Signature.RecordID,
		Passthrough: v.Passthrough,
		CreatedAt:   v.CreatedAt,
	}
	PopulateFiberMatchKeys(&p, &(v.MatchKeys))
	return p
}

func PopulateFiberMatchKeys(target *HouseHoldFiberDS, source *HouseHoldOutput) {
	KeyList := structs.Names(&HouseHoldOutput{})
	for _, key := range KeyList {
		SetHouseHoldFiberMatchKeyField(target, key, GetMatchKeyFieldFromHouseHoldOutput(source, key))
	}
}

func SetHouseHoldFiberMatchKeyField(v *HouseHoldFiberDS, field string, value MatchKeyField) {
	r := reflect.ValueOf(v)
	f := reflect.Indirect(r).FieldByName(field)
	f.Set(reflect.ValueOf(value))
	LogDev(fmt.Sprintf("SetHouseHoldFiberMatchKeyField: %v %v", field, value))
}

func GetMatchKeyFieldFromPeopleOutput(v *PeopleOutput, field string) MatchKeyField {
	r := reflect.ValueOf(v)
	f := reflect.Indirect(r).FieldByName(field)
	return f.Interface().(MatchKeyField)
}

func GetMatchKeyFieldFromHouseHoldOutput(v *HouseHoldOutput, field string) MatchKeyField {
	r := reflect.ValueOf(v)
	f := reflect.Indirect(r).FieldByName(field)
	return f.Interface().(MatchKeyField)
}

func LogDev(s string) {
	if dev {
		log.Printf(s)
	}
}

func GetMatchKey360ByName(v []MatchKey360, key string) *MatchKey360 {
	for _, m := range v {
		if m.Key == key {
			return &m
		}
	}
	return &MatchKey360{}
}

func GetMatchKeyFieldFromDSFiber(v *HouseHoldFiberDS, field string) MatchKeyField {
	r := reflect.ValueOf(v)
	f := reflect.Indirect(r).FieldByName(field)
	return f.Interface().(MatchKeyField)
}

func PopulateSetOutputSignatures(target *HouseHoldSetDS, values []Signature) {
	KeyList := structs.Names(&Signature{})
	for _, key := range KeyList {
		SetHouseHold360SetOutputFieldValues(target, key, GetSignatureSliceValues(values, key))
		if key == "RecordID" {
			SetHouseHold360SetOutputFieldValues(target, key+"Normalized", GetRecordIDNormalizedSliceValues(values, key))
		}
	}
}

func SetHouseHold360SetOutputFieldValues(v *HouseHoldSetDS, field string, value []string) {
	r := reflect.ValueOf(v)
	f := reflect.Indirect(r).FieldByName(field)
	f.Set(reflect.ValueOf(value))
	LogDev(fmt.Sprintf("SetHouseHold360SetOutputFieldValues: %v %v", field, value))
}

func SetHouseHold360GoldenOutputFieldValue(v *HouseHoldGoldenDS, field string, value string) {
	r := reflect.ValueOf(v)
	f := reflect.Indirect(r).FieldByName(field)
	f.Set(reflect.ValueOf(value))
	LogDev(fmt.Sprintf("SetHouseHold360GoldenOutputFieldValue: %v %v", field, value))
}

func GetRecordIDNormalizedSliceValues(source []Signature, field string) []string {
	slice := []string{}
	for _, s := range source {
		slice = append(slice, Left(GetSignatureField(&s, field), 36))
	}
	return slice
}

func GetSignatureSliceValues(source []Signature, field string) []string {
	slice := []string{}
	for _, s := range source {
		slice = append(slice, GetSignatureField(&s, field))
	}
	return slice
}

func GetSignatureField(v *Signature, field string) string {
	r := reflect.ValueOf(v)
	f := reflect.Indirect(r).FieldByName(field)
	return f.Interface().(string)
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

func PopulateSetOutputMatchKeys(target *HouseHoldSetDS, values []MatchKey360) {
	KeyList := structs.Names(&PeopleOutput{})
	for _, key := range KeyList {
		SetHouseHold360SetOutputFieldValues(target, key, GetSetValuesFromMatchKeys(values, key))
		SetHouseHold360SetOutputFieldValues(target, key+"Normalized", GetSetValuesFromMatchKeysNormalized(values, key))
	}
}

func PopulateGoldenOutputMatchKeys(target *HouseHoldGoldenDS, values []MatchKey360) {
	KeyList := structs.Names(&PeopleOutput{})
	for _, key := range KeyList {
		SetHouseHold360GoldenOutputFieldValue(target, key, GetGoldenValueFromMatchKeys(values, key))
	}
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

func GetGoldenValueFromMatchKeys(values []MatchKey360, key string) string {
	for _, m := range values {
		if m.Key == key {
			return m.Value
		}
	}
	return ""
}
