package recordprocessor

// touch

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"cloud.google.com/go/datastore"
	"cloud.google.com/go/pubsub"

	"github.com/gomodule/redigo/redis"
)

// PubSubMessage is the payload of a pubsub event
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

type Input struct {
	Signature   Signature         `json:"signature"`
	Passthrough map[string]string `json:"passthrough"`
	Fields      map[string]string `json:"fields"`
	Attributes  map[string]string `json:"attributes"`
}

type Immutable struct {
	EventType string            `datastore:"Type"`
	EventID   string            `datastore:"EventID"`
	RecordID  string            `datastore:"RecordID"`
	Fields    map[string]string `datastore:"Fields"`
	TimeStamp time.Time         `datastore:"Created"`
}

type ImmutableDS struct {
	EventType string    `datastore:"Type"`
	EventID   string    `datastore:"EventID"`
	RecordID  string    `datastore:"RecordID"`
	Fields    []KVP     `datastore:"Fields,noindex"`
	TimeStamp time.Time `datastore:"Created"`
}

// NERcolumns coloumns for NER
type NERcolumns struct {
	ColumnName  string             `json:"ColumnName"`
	NEREntities map[string]float64 `json:"NEREntities"`
}

//NERresponse response
type NERresponse struct {
	Columns     []NERcolumns `json:"Columns"`
	ElapsedTime float64      `json:"ElapsedTime"`
	Owner       string       `json:"Owner"`
	Source      string       `json:"Source"`
	TimeStamp   string       `json:"TimeStamp"`
}

type NERCache struct {
	Columns      []NERcolumns `json:"columns"`
	TimeStamp    time.Time    `json:"time"`
	ApplyCounter int          `json:"counter"`
	Recompute    bool         `json:"dirty"`
	Source       string       `json:"source`
}

type KVP struct {
	Key   string `json:"k" datastore:"k"`
	Value string `json:"v" datastore:"v"`
}

// NERrequest request
type NERrequest struct {
	Owner  string
	Source string
	Data   map[string][]string
}

type Output struct {
	Signature   Signature         `json:"signature"`
	Passthrough map[string]string `json:"passthrough"`
	Fields      map[string]string `json:"fields"`
	Attributes  map[string]string `json:"attributes"`
}

// ProjectID is the env var of project id
var ProjectID = os.Getenv("PROJECTID")
var DSNameSpace = os.Getenv("DATASTORENS")
var RedisAddress = os.Getenv("MEMSTORE")
var PubSubTopic = os.Getenv("PSOUTPUT")
var NERThreshold = 1000
var NERApi = os.Getenv("NERAPI")
var Env = os.Getenv("ENVIRONMENT")
var DSKind = os.Getenv("DSKIND")

var ds *datastore.Client
var ps *pubsub.Client
var msPool *redis.Pool
var topic *pubsub.Topic

func init() {
	ctx := context.Background()
	ds, _ = datastore.NewClient(ctx, ProjectID)
	ps, _ = pubsub.NewClient(ctx, ProjectID)
	msPool = NewPool(RedisAddress)
	topic = ps.Topic(PubSubTopic)

	log.Printf("init completed, pubsub topic name: %v", topic)
}

func ProcessRecord(ctx context.Context, m PubSubMessage) error {
	var input Input
	if err := json.Unmarshal(m.Data, &input); err != nil {
		log.Fatalf("Unable to unmarshal message %v with error %v", string(m.Data), err)
	}

	if len(input.Fields) > 0 {
		for k, v := range input.Fields {
			input.Fields[k] = strings.TrimSpace(v)
			if strings.EqualFold(input.Fields[k], "NULL") {
				input.Fields[k] = ""
			}
		}
	} else {
		// empty field list
		return nil
	}

	// first construct the Immutable record
	immutable := Immutable{
		EventID:   input.Signature.EventID,
		EventType: input.Signature.EventType,
		RecordID:  input.Signature.RecordID,
		Fields:    input.Fields,
		TimeStamp: time.Now(),
	}

	immutableDS := ImmutableDS{
		EventID:   immutable.EventID,
		EventType: immutable.EventType,
		RecordID:  immutable.RecordID,
		Fields:    ToKVPSlice(&immutable.Fields),
		TimeStamp: immutable.TimeStamp,
	}

	// store this in DS
	dsKey := datastore.IncompleteKey(DSKind, nil)
	dsKey.Namespace = strings.ToLower(fmt.Sprintf("%v-%v", Env, input.Signature.OwnerID))
	if _, err := ds.Put(ctx, dsKey, &immutableDS); err != nil {
		log.Fatalf("Exception storing record kind %v sig %v, error %v", DSKind, input.Signature, err)
	}

	// check if NER exists
	NERKey := GetNERKey(input.Signature, GetMapKeys(input.Fields))
	NER := FindNER(NERKey)
	if len(NER.Columns) > 0 {
		// found ner
		if NER.Source == "FILE" {
			// no need to recompute
		} else if NER.ApplyCounter > NERThreshold {
			// need to recompute NER
			var entities []Immutable
			query := datastore.NewQuery(DSKind).Namespace(DSNameSpace)
			query.Order("-created").Limit(100)

			if _, err := ds.GetAll(ctx, query, &entities); err != nil {
				// TODO: address field fluctuations
				NERInput := make(map[string][]string)
				for _, e := range entities {
					for k, v := range e.Fields {
						if len(v) > 0 {
							NERInput[k] = append(NERInput[k], v)
						}
					}
				}

				// Call NER API
				NerRequest := NERrequest{
					Owner:  fmt.Sprintf("%v", input.Signature.OwnerID),
					Source: "wemade",
					Data:   NERInput,
				}
				log.Printf("%v Getting NER responses", input.Signature.EventID)
				NerResponse := GetNERresponse(NerRequest)

				// Store NER in Redis if we have a NER
				NerKey := GetNERKey(input.Signature, GetMapKeysFromSlice(NERInput))
				if len(NerResponse.Owner) > 0 {
					PersistNER(NerKey, NerResponse)
				}
			}

		}
	}

	// pub the record without attributes appended to fields
	var output Output
	output.Signature = input.Signature
	output.Fields = input.Fields
	output.Attributes = input.Attributes
	output.Passthrough = input.Passthrough

	// let's pub it
	outputJSON, _ := json.Marshal(output)

	// push into pubsub
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

func GetMapKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func GetMapKeysFromSlice(m map[string][]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func GetLowerCaseSorted(m []string) []string {
	var result []string
	for _, k := range m {
		result = append(result, strings.ToLower(k))
	}
	sort.Strings(result)
	return result
}

func GetNERKey(sig Signature, columns []string) string {
	// concatenate all columnn headers together, in lower case
	keys := strings.Join(GetLowerCaseSorted(columns[:]), "-")
	hasher := sha1.New()
	io.WriteString(hasher, keys)
	return fmt.Sprintf("ner:%v:%v:%v:%x", sig.OwnerID, strings.ToLower(sig.Source), strings.ToLower(sig.EventType), hasher.Sum(nil))
}

func FindNER(key string) NERCache {
	var ner NERCache
	ms := msPool.Get()
	s, err := redis.String(ms.Do("GET", key))
	if err == nil {
		json.Unmarshal([]byte(s), &ner)
	}
	return ner
}

func GetNERresponse(nerData NERrequest) NERresponse {
	jsonValue, err := json.Marshal(nerData)
	log.Printf("calling NER endpoint with %v", nerData)
	if err != nil {
		log.Panicf("Could not convert the NERrequest to json: %v", err)
	}
	var structResponse NERresponse
	response, err := http.Post(NERApi, "application/json", bytes.NewBuffer(jsonValue))
	if err != nil {
		log.Fatalf("The NER request failed: %v", err)
	} else {
		if response.StatusCode != 200 {
			log.Fatalf("NER request failed, status code:%v", response.StatusCode)
		}
		data, err := ioutil.ReadAll(response.Body)
		if err != nil {
			log.Fatalf("Couldn't read the NER response: %v", err)
		}
		log.Printf("ner response %v", string(data))
		json.Unmarshal(data, &structResponse)
	}
	return structResponse
}

func PersistNER(key string, ner NERresponse) {
	ms := msPool.Get()
	var cache NERCache

	cache.Columns = ner.Columns
	cache.TimeStamp = time.Now()
	cache.Recompute = false
	cache.Source = "STREAMING"

	cacheJSON, _ := json.Marshal(cache)
	_, err := ms.Do("SET", key, string(cacheJSON))
	if err != nil {
		log.Fatalf("error storing NER %v", err)
	}
}

func NewPool(addr string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial:        func() (redis.Conn, error) { return redis.Dial("tcp", addr) },
	}
}

func ToJson(v *map[string]string) string {
	jsonString, err := json.Marshal(v)
	if err == nil {
		return string(jsonString)
	} else {
		log.Fatalf("%v Could not convert map %v to json: %v", v, err)
		return ""
	}

}

func ToKVPSlice(v *map[string]string) []KVP {
	var result []KVP
	for k, v := range *v {
		result = append(result, KVP{
			Key:   k,
			Value: v,
		})
	}
	return result
}
