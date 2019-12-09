package consignment360

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

	"cloud.google.com/go/datastore"
	"cloud.google.com/go/pubsub"

	"github.com/fatih/structs"
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
	RecordID  string `json:"recordId"`
}

type ConsignmentInput struct {
	Signature   Signature         `json:"signature"`
	Passthrough map[string]string `json:"passthrough"`
	MatchKeys   ConsignmentOutput `json:"matchkeys`
}

type ConsignmentFiber struct {
	Signature   Signature         `json:"signature"`
	Passthrough []Passthrough360  `json:"passthrough"`
	MatchKeys   ConsignmentOutput `json:"matchkeys"`
	ID          string            `json:"fiberId"`
	CreatedAt   time.Time         `json:"createdAt"`
}

type ConsignmentFiberDS struct {
	ID            *datastore.Key   `datastore:"__key__"`
	CreatedAt     time.Time        `datastore:"createdAt"`
	OwnerID       string           `datastore:"ownerid"`
	Source        string           `datastore:"source"`
	EventID       string           `datastore:"eventid"`
	EventType     string           `datastore:"eventtype"`
	RecordID      string           `datastore:"recordid"`
	ORDERID       MatchKeyField    `datastore:"orderid"`
	CONSIGNMENTID MatchKeyField    `datastore:"consignmentid"`
	SHIPDATE      MatchKeyField    `datastore:"shipDate"`
	Passthrough   []Passthrough360 `datastore:"passthrough"`
}

type MatchKeyField struct {
	Value  string `json:"value"`
	Source string `json:"source"`
	Type   string `json:"type"`
}

type ConsignmentOutput struct {
	CONSIGNMENTID MatchKeyField `json:"id"`
	ORDERID       MatchKeyField `json:"orderId"`
	SHIPDATE      MatchKeyField `json:"shipDate"`
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

type Consignment360Output struct {
	ID           string           `json:"id"`
	Signature    Signature360     `json:"signature"`
	Signatures   []Signature      `json:"signatures"`
	CreatedAt    time.Time        `json:"createdAt"`
	TimeStamp    time.Time        `json:"timestamp"`
	Fibers       []string         `json:"fibers"`
	Passthroughs []Passthrough360 `json:"passthroughs"`
	MatchKeys    []MatchKey360    `json:"matchKeys"`
}

type ConsignmentSetDS struct {
	ID                      *datastore.Key `datastore:"__key__"`
	OwnerID                 []string       `datastore:"ownerid"`
	Source                  []string       `datastore:"source"`
	EventID                 []string       `datastore:"eventid"`
	EventType               []string       `datastore:"eventtype"`
	RecordID                []string       `datastore:"recordid"`
	RecordIDNormalized      []string       `datastore:"recordidnormalized"`
	CreatedAt               time.Time      `datastore:"createdat"`
	Fibers                  []string       `datastore:"fibers"`
	ORDERID                 string         `datastore:"orderid"`
	ORDERIDNormalized       string         `datastore:"orderidnormalized"`
	CONSIGNMENTID           string         `datastore:"consignmentid"`
	CONSIGNMENTIDNormalized string         `datastore:"consignmentidnormalized"`
	SHIPDATE                string         `datastore:"shipDate"`
	SHIPDATENormalized      string         `datastore:"shipDatenormalized"`
}

type ConsignmentGoldenDS struct {
	ID            *datastore.Key `datastore:"__key__"`
	CreatedAt     time.Time      `datastore:"createdat"`
	ORDERID       string         `datastore:"orderid"`
	CONSIGNMENTID string         `datastore:"consignmentid"`
	SHIPDATE      string         `datastore:"shipDate"`
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

var ds *datastore.Client

// var setSchema bigquery.Schema

func init() {
	ctx := context.Background()
	ps, _ = pubsub.NewClient(ctx, ProjectID)
	topic = ps.Topic(PubSubTopic)
	topic2 = ps.Topic(PubSubTopic2)
	ds, _ = datastore.NewClient(ctx, ProjectID)

	log.Printf("init completed, pubsub topic name: %v", topic)
}

func Consignment360(ctx context.Context, m PubSubMessage) error {
	var input ConsignmentInput
	if err := json.Unmarshal(m.Data, &input); err != nil {
		log.Fatalf("Unable to unmarshal message %v with error %v", string(m.Data), err)
	}

	// if we don't have a matchable key... drop!!
	if input.MatchKeys.CONSIGNMENTID.Value == "" {
		return nil
	}

	// store the fiber
	OutputPassthrough := ConvertPassthrough(input.Passthrough)
	var fiber ConsignmentFiber
	fiber.CreatedAt = time.Now()
	fiber.ID = uuid.New().String()
	fiber.MatchKeys = input.MatchKeys
	fiber.Passthrough = OutputPassthrough
	fiber.Signature = input.Signature

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
	if len(input.Signature.RecordID) == 0 {
		// ensure record id is not blank or we'll have problem
		input.Signature.RecordID = uuid.New().String()
	}
	MatchByValue0 := input.Signature.RecordID

	MatchByKey1 := "ORDERID"
	MatchByValue1 := strings.Replace(input.MatchKeys.ORDERID.Value, "'", `''`, -1)

	MatchByKey2 := "CONSIGNMENTID"
	MatchByValue2 := strings.Replace(input.MatchKeys.CONSIGNMENTID.Value, "'", `''`, -1)

	matchedSets := []ConsignmentSetDS{}
	queriedSets := []ConsignmentSetDS{}
	setQuery := datastore.NewQuery(DSKindSet).Namespace(dsNameSpace).Filter("recordid =", MatchByValue0)
	if _, err := ds.GetAll(ctx, setQuery, &queriedSets); err != nil {
		log.Fatalf("Error querying sets query 1: %v", err)
	} else {
		for _, s := range queriedSets {
			matchedSets = append(matchedSets, s)
		}
	}
	if len(MatchByValue1) > 0 {
		setQuery := datastore.NewQuery(DSKindSet).Namespace(dsNameSpace).Filter(strings.ToLower(MatchByKey1)+"normalized =", strings.ToUpper(MatchByValue1))
		if _, err := ds.GetAll(ctx, setQuery, &queriedSets); err != nil {
			log.Fatalf("Error querying sets query 1: %v", err)
		} else {
			for _, s := range queriedSets {
				matchedSets = append(matchedSets, s)
			}
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
	var Fibers []ConsignmentFiberDS
	for _, fiber := range matchedFibers {
		dsFiberGetKey := datastore.NameKey(DSKindFiber, fiber, nil)
		dsFiberGetKey.Namespace = dsNameSpace
		FiberKeys = append(FiberKeys, dsFiberGetKey)
		Fibers = append(Fibers, ConsignmentFiberDS{})
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

	var output Consignment360Output
	var FiberSignatures []Signature
	output.ID = uuid.New().String()
	MatchKeyList := structs.Names(&ConsignmentOutput{})
	FiberMatchKeys := make(map[string][]string)
	// collect all fiber match key values
	for _, name := range MatchKeyList {
		FiberMatchKeys[name] = []string{}
	}
	//var SetMembers []ConsignmentSetMember
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
		mk.Value = GetMatchKeyFieldFromStruct(&input.MatchKeys, name).Value
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
		mk.Value = strings.TrimSpace(GetMatchKeyFieldFromStruct(&input.MatchKeys, name).Value)
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

	// record the set id in DS
	var setDS ConsignmentSetDS
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

	var goldenDS ConsignmentGoldenDS
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
			"type":   "consignment",
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
			"type":   "consignment",
			"source": "360",
		},
	})

	return nil
}

func GetMkField(v *ConsignmentOutput, field string) MatchKeyField {
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

func GetFiberDS(v *ConsignmentFiber) ConsignmentFiberDS {
	p := ConsignmentFiberDS{
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

func PopulateFiberMatchKeys(target *ConsignmentFiberDS, source *ConsignmentOutput) {
	KeyList := structs.Names(&ConsignmentOutput{})
	for _, key := range KeyList {
		SetConsignmentFiberMatchKeyField(target, key, GetMatchKeyFieldFromStruct(source, key))
	}
}

func GetMatchKeyFieldFromStruct(v *ConsignmentOutput, field string) MatchKeyField {
	r := reflect.ValueOf(v)
	f := reflect.Indirect(r).FieldByName(field)
	return f.Interface().(MatchKeyField)
}

func SetConsignmentFiberMatchKeyField(v *ConsignmentFiberDS, field string, value MatchKeyField) {
	r := reflect.ValueOf(v)
	f := reflect.Indirect(r).FieldByName(field)
	f.Set(reflect.ValueOf(value))
	LogDev(fmt.Sprintf("SetConsignmentFiberMatchKeyField: %v %v", field, value))
}

func LogDev(s string) {
	if dev {
		log.Printf(s)
	}
}

func GetMatchKeyFieldFromDSFiber(v *ConsignmentFiberDS, field string) MatchKeyField {
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

func PopulateSetOutputSignatures(target *ConsignmentSetDS, values []Signature) {
	KeyList := structs.Names(&Signature{})
	for _, key := range KeyList {
		SetConsignment360SetOutputFieldValues(target, key, GetSignatureSliceValues(values, key))
	}
}

func PopulateSetOutputMatchKeys(target *ConsignmentSetDS, values []MatchKey360) {
	KeyList := structs.Names(&ConsignmentOutput{})
	for _, key := range KeyList {
		SetConsignment360SetOutputFieldValues(target, key, GetSetValuesFromMatchKeys(values, key))
		SetConsignment360SetOutputFieldValues(target, key+"Normalized", GetSetValuesFromMatchKeysNormalized(values, key))
	}
}

func PopulateGoldenOutputMatchKeys(target *ConsignmentGoldenDS, values []MatchKey360) {
	KeyList := structs.Names(&ConsignmentOutput{})
	for _, key := range KeyList {
		SetConsignment360GoldenOutputFieldValue(target, key, GetGoldenValueFromMatchKeys(values, key))
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

func SetConsignment360SetOutputFieldValues(v *ConsignmentSetDS, field string, value []string) {
	r := reflect.ValueOf(v)
	f := reflect.Indirect(r).FieldByName(field)
	f.Set(reflect.ValueOf(value))
	LogDev(fmt.Sprintf("SetConsignment360SetOutputFieldValues: %v %v", field, value))
}

func SetConsignment360GoldenOutputFieldValue(v *ConsignmentGoldenDS, field string, value string) {
	r := reflect.ValueOf(v)
	f := reflect.Indirect(r).FieldByName(field)
	f.Set(reflect.ValueOf(value))
	LogDev(fmt.Sprintf("SetConsignment360GoldenOutputFieldValue: %v %v", field, value))
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
