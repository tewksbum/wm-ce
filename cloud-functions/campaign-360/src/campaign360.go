package campaign360

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

type CampaignInput struct {
	Signature   Signature         `json:"signature"`
	Passthrough map[string]string `json:"passthrough"`
	MatchKeys   CampaignOutput    `json:"matchkeys`
}

type MatchKeyField struct {
	Value  string `json:"value"`
	Source string `json:"source"`
}

type CampaignOutput struct {
	CAMPAIGNID MatchKeyField `json:"campaignId"`

	NAME      MatchKeyField `json:"name"`
	TYPE      MatchKeyField `json:"type"`
	CHANNEL   MatchKeyField `json:"channel"`
	STARTDATE MatchKeyField `json:"startDate"`
	ENDDATE   MatchKeyField `json:"endDate"`
	BUDGET    MatchKeyField `json:"budget"`
}

type Signature360 struct {
	OwnerID   int64  `json:"ownerId" bigquery:"ownerId"`
	Source    string `json:"source" bigquery:"source"`
	EventID   string `json:"eventId" bigquery:"eventId"`
	EventType string `json:"eventType" bigquery:"eventType"`
}

type MatchKey360 struct {
	Key   string `json:"key" bigquery:"key"`
	Type  string `json:"type" bigquery:"type"`
	Value string `json:"value" bigquery:"value"`
	//Values []string `json:"values" bigquery:"values"`
}

type Passthrough360 struct {
	Name  string `json:"name" bigquery:"name"`
	Value string `json:"value" bigquery:"value"`
}

type Campaign360Output struct {
	ID           string           `json:"id" bigquery:"id"`
	Signature    Signature360     `json:"signature" bigquery:"signature"`
	Signatures   []Signature      `json:"signatures" bigquery:"signatures"`
	CreatedAt    time.Time        `json:"createdAt" bigquery:"createdAt"`
	Fibers       []string         `json:"fibers" bigquery:"fibers"`
	Passthroughs []Passthrough360 `json:"passthroughs" bigquery:"passthroughs"`
	MatchKeys    []MatchKey360    `json:"matchKeys" bigquery:"matchKeys"`

	TrustedIDs []string `json:"trustedId" bigquery:"trustedId"`
}

var ProjectID = os.Getenv("PROJECTID")
var PubSubTopic = os.Getenv("PSOUTPUT")
var BigQueryTable = os.Getenv("BQTABLE")

var ps *pubsub.Client
var topic *pubsub.Topic

var bq bigquery.Client
var bs bigquery.Schema

// var setSchema bigquery.Schema

func init() {
	ctx := context.Background()
	ps, _ = pubsub.NewClient(ctx, ProjectID)
	topic = ps.Topic(PubSubTopic)
	bq, _ := bigquery.NewClient(ctx, ProjectID)
	bs, _ := bigquery.InferSchema(Campaign360Output{})

	log.Printf("init completed, pubsub topic name: %v, bq client: %v, bq schema: %v", topic, bq, bs)
}

func Campaign360(ctx context.Context, m PubSubMessage) error {
	var input CampaignInput
	if err := json.Unmarshal(m.Data, &input); err != nil {
		log.Fatalf("Unable to unmarshal message %v with error %v", string(m.Data), err)
	}

	// locate by key (trusted id)
	setMeta := &bigquery.TableMetadata{
		Schema: bs,
	}
	DatasetID := strconv.FormatInt(input.Signature.OwnerID, 10)
	// make sure dataset exists
	dsmeta := &bigquery.DatasetMetadata{
		Location: "US", // Create the dataset in the US.
	}
	if err := bq.Dataset(DatasetID).Create(ctx, dsmeta); err != nil {
	}
	setTable := bq.Dataset(DatasetID).Table(BigQueryTable)
	if err := setTable.Create(ctx, setMeta); err != nil {
	}

	// locate existing set
	MatchByKey := "trustedId"
	MatchByValue := strings.Replace(input.MatchKeys.CAMPAIGNID.Value, "'", "\\'", -1)
	QueryText := fmt.Sprintf("SELECT * FROM `%s.%s.%s`, UNNEST(%s) u WHERE u = '%s' ", ProjectID, DatasetID, BigQueryTable, MatchByKey, MatchByValue)
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

	// only need the first value
	var output Campaign360Output
	err = BQIterator.Next(&output)
	if err == iterator.Done {
	} else if err != nil {
		log.Fatalf("%v bq returned value not matching expected type: %v", input.Signature.EventID, err)
		return err
	}

	// append to the output value
	output.ID = uuid.New().String()
	output.Signatures = append(output.Signatures, input.Signature)
	output.Signature = Signature360{
		OwnerID:   input.Signature.OwnerID,
		Source:    input.Signature.Source,
		EventID:   input.Signature.EventID,
		EventType: input.Signature.EventType,
	}
	output.CreatedAt = time.Now()
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
	output.TrustedIDs = append(output.TrustedIDs, input.MatchKeys.CAMPAIGNID.Value)
	FieldNames := structs.Names(&CampaignOutput{})
	for _, name := range FieldNames {
		mk := MatchKey360{
			Key:   name,
			Value: GetMkField(&input.MatchKeys, name).Value,
		}
		output.MatchKeys = append(output.MatchKeys, mk)
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

func GetMkField(v *CampaignOutput, field string) MatchKeyField {
	r := reflect.ValueOf(v)
	f := reflect.Indirect(r).FieldByName(field)
	return f.Interface().(MatchKeyField)
}
