// Package abmMapper
package abmMapper

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"log"
	"os"
	"text/template"

	"cloud.google.com/go/datastore"
)

// ProjectID is the GCP Project ID
var ProjectID = os.Getenv("PROJECTID")

// KindTemplate is the kind to write the original records to
var KindTemplate = os.Getenv("KINDTEMPLATTE")

// KindRecordTemplate is the kind to write the original records to
var Namespace = os.Getenv("NAMESPACE")

type Signature struct {
	OwnerID   int64  `json:"ownerId"`
	Source    string `json:"source"`
	EventID   string `json:"eventId"`
	EventType string `json:"eventType"`
	RecordID  string `json:"recordId"`
}

type MatchKeyField struct {
	Value  string `json:"value" bigquery:"value"`
	Source string `json:"source" bigquery:"source"`
	Type   string `json:"type" bigquery:"type"`
}

// Owner     When we receive messages for this owner, we will evaluate
// Source    The source system to which we want to push data
// Mapping   For every matchKey… listing of mapped sourceKey
type PubSubMapping struct {
	Signature   Signature                `json:"signature"`
	Passthrough map[string]string        `json:"passthrough"`
	MatchKeys   map[string]MatchKeyField `json:"matchkeys"`
}

type MatchKeyValue struct {
	MatchKey   string
	Source     string
	Type       string
	EntityType string
}

// PubSubMessage is the payload of a Pub/Sub event.
type PubSubMessage struct {
	Data       []byte            `json:"data"`
	Attributes map[string]string `json:"attributes"`
}

func getOutputHash(m MatchKeyValue) string {
	var text string
	text = m.MatchKey + m.Source + m.Type
	hasher := md5.New()
	hasher.Write([]byte(text))
	return hex.EncodeToString(hasher.Sum(nil))
}
func Main(ctx context.Context, m PubSubMessage) error {
	var input PubSubMapping
	if err := json.NewDecoder(bytes.NewBuffer(m.Data)).Decode(&input); err != nil {
		log.Printf("There was an issue decoding the message %v", m.Data)
		return err
	}
	if input.Signature.OwnerID == 0 {
		log.Fatalf("Empty owner, discarding message %v", input.Signature)
		return nil
	}

	inputType := m.Attributes["type"]
	inputSource := m.Attributes["source"]

	log.Printf("Received input from %v at %v with type %v from %v", input.Signature.OwnerID, input.Signature.Source, inputType, inputSource)
	dsClient, err := datastore.NewClient(ctx, ProjectID)
	if err != nil {
		log.Printf("Error accessing datastore: %v", err)
		return err
	}

	var kind bytes.Buffer
	dsKindtemplate, err := template.New("abmOwnerSource").Parse(KindTemplate)
	if err != nil {
		log.Printf("%v %v Unable to parse text template: %v", input.Signature.OwnerID, input.Signature.Source, err)
		return err
	}
	if err := dsKindtemplate.Execute(&kind, input.Signature); err != nil {
		log.Printf("%v %v Unable to parse text template: %v", input.Signature.OwnerID, input.Signature.Source, err)
		return err
	}
	log.Printf("Mapping mks at <%v> namespace with kind <%v> ", Namespace, kind.String())
	var OutputMatchKeyValue []MatchKeyValue
	var dskeys []*datastore.Key
	for key, match := range input.MatchKeys {
		var matchKeyValue MatchKeyValue
		matchKeyValue.MatchKey = key
		matchKeyValue.Source = match.Source
		matchKeyValue.Type = match.Type
		matchKeyValue.EntityType = inputType
		OutputMatchKeyValue = append(OutputMatchKeyValue, matchKeyValue)
		// Hash the result to avoid duplication
		sourceKeyHash := getOutputHash(matchKeyValue)
		sourceKey := datastore.NameKey(kind.String(), sourceKeyHash, nil)
		sourceKey.Namespace = Namespace
		dskeys = append(dskeys, sourceKey)
	}

	if len(OutputMatchKeyValue) == 0 {
		log.Printf("Empty output, nothing to map, Input: %v", input.MatchKeys)
		return nil
	}

	log.Printf("Storing at <%v> namespace with kind <%v>: %v", Namespace, kind.String(), OutputMatchKeyValue)
	multiLimit := 500
	for i := 0; i < len(OutputMatchKeyValue); i += multiLimit {
		end := i + multiLimit

		if end > len(OutputMatchKeyValue) {
			end = len(OutputMatchKeyValue)
		}
		_, err = dsClient.PutMulti(ctx, dskeys[i:end], OutputMatchKeyValue[i:end])
		if err != nil {
			log.Printf("Error storing MatchKeys: %v", err)
			return err
		}
	}
	return nil
}
