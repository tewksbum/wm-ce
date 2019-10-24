package jsonprocessor

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"reflect"
	"strings"

	"cloud.google.com/go/pubsub"
)

// BLACKLIST is a list of json nodes that will be ignored
var BLACKLIST = []string{"VENDOR"}

// PubSubMessage is the payload of a pubsub event
type PubSubMessage struct {
	Data []byte `json:"data"`
}

type Signature struct {
	OwnerID   int64  `json:"ownerId"`
	Source    string `json:"source"`
	EventID   string `json:"eventId"`
	EventType string `json:"eventType"`
}

type Input struct {
	Signature   Signature              `json:"signature"`
	Passthrough map[string]string      `json:"passthrough"`
	Attributes  map[string]string      `json:"attributes"`
	EventData   map[string]interface{} `json:"eventData"`
}

type Output struct {
	Signature   Signature         `json:"signature"`
	Passthrough map[string]string `json:"passthrough"`
	Fields      map[string]string `json:"fields"`
}

// ProjectID is the env var of project id
var ProjectID = os.Getenv("PROJECTID")

var PubSubTopic = os.Getenv("PSOUTPUT")

// global vars
var ctx context.Context
var ps *pubsub.Client
var topic *pubsub.Topic

func init() {
	ctx = context.Background()
	ps, _ = pubsub.NewClient(ctx, ProjectID)
	topic = ps.Topic(PubSubTopic)
	log.Printf("init completed, pubsub topic name: %v", topic)
}

/* JsonProcess Takes in a json string
Recursively locates nested nodes
And Pubs to a topic
*/
func ProcessJson(ctx context.Context, m PubSubMessage) error {
	var input Input
	if err := json.Unmarshal(m.Data, &input); err != nil {
		log.Fatalf("Unable to unmarshal message %v with error %v", string(m.Data), err)
	}

	// name the root object by EventType
	UnNest(input.Signature, input.Passthrough, input.Attributes, input.Signature.EventType, input.EventData)
	return nil
}

func UnNest(sig Signature, ps map[string]string, attr map[string]string, prefix string, data map[string]interface{}) {
	fields := make(map[string]string)

	for key, raw := range data {
		var mapKey = strings.ToUpper(key)
		if SliceContains(BLACKLIST, mapKey) {
			continue
		}
		UnNestField(fields, prefix+"."+key, reflect.ValueOf(raw), sig, ps, attr)
	}

	// append attributes
	for key, value := range attr {
		fields["Attr."+key] = value
	}

	var output Output
	output.Signature = sig
	output.Fields = fields
	output.Passthrough = ps

	// let's pub it
	outputJSON, _ := json.Marshal(output)

	// push into pubsub
	psresult := topic.Publish(ctx, &pubsub.Message{
		Data: outputJSON,
	})

	psid, err := psresult.Get(ctx)
	_, err = psresult.Get(ctx)
	if err != nil {
		log.Fatalf("%v Could not pub to pubsub: %v", sig.EventID, err)
	} else {
		log.Printf("%v pubbed record as message id %v: %v", sig.EventID, psid, string(outputJSON))
	}
	return
}

func UnNestField(result map[string]string, prefix string, v reflect.Value, sig Signature, ps map[string]string, attr map[string]string) {
	if v.Kind() == reflect.Interface {
		v = v.Elem()
	}

	switch v.Kind() {
	case reflect.Bool:
		if v.Bool() {
			result[prefix] = "true"
		} else {
			result[prefix] = "false"
		}
	case reflect.Int:
		result[prefix] = fmt.Sprintf("%d", v.Int())
	case reflect.Float32:
	case reflect.Float64:
		result[prefix] = fmt.Sprintf("%f", v.Float())
	case reflect.Map:

		UnNestMap(prefix, v, sig, ps, attr)
	case reflect.Slice:
		UnNestSlice(prefix, v, sig, ps, attr)
	case reflect.String:
		result[prefix] = v.String()
	case reflect.Invalid:
		return
	default:
		log.Fatalf("Unknown data: %s", v.Kind())
	}
}

// UnNestMap checks key name against blacklist, and ensure all field keys are string
func UnNestMap(prefix string, v reflect.Value, sig Signature, ps map[string]string, attr map[string]string) {
	for _, k := range v.MapKeys() {
		if k.Kind() == reflect.Interface {
			k = k.Elem()
		}

		if k.Kind() != reflect.String {
			log.Fatalf("%s: map key is not string: %s", prefix, k)
			return
		}
	}
	UnNest(sig, ps, attr, prefix, v.Interface().(map[string]interface{}))
}

// UnNestSlice checks key name against blacklist, and ensure all field keys are strings
func UnNestSlice(prefix string, v reflect.Value, sig Signature, ps map[string]string, attr map[string]string) {

	for i := 0; i < v.Len(); i++ {
		vi := v.Index(i)
		if vi.Kind() == reflect.Interface {
			vi = vi.Elem()
		}
		if vi.Kind() != reflect.Map {
			continue
		}
		// TODO: how do we handle slice that is not a map, say slice of strings
		UnNestMap(fmt.Sprintf("%s.%d", prefix, i), vi, sig, ps, attr)
	}
}

func SliceContains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}
