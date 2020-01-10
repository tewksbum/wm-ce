package orderpost

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"reflect"

	"cloud.google.com/go/pubsub"
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

type Prediction struct {
	Predictions []float64 `json:"predictions"`
}

type InputColumn struct {
	NER      NER      `json:"NER"`
	OrderERR OrderERR `json:"OrderERR"`
	Name     string   `json:"Name"`
	Value    string   `json:"Value"`
	MatchKey string   `json:"MK"`
}

type Input struct {
	Signature   Signature         `json:"signature"`
	Passthrough map[string]string `json:"passthrough"`
	Prediction  Prediction        `json:"prediction`
	Columns     []InputColumn     `json:"columns`
}

type Output struct {
	Signature   Signature         `json:"signature"`
	Passthrough map[string]string `json:"passthrough"`
	MatchKeys   OrderOutput       `json:"matchkeys`
}

type MatchKeyField struct {
	Value  string `json:"value"`
	Source string `json:"source"`
}

type OrderOutput struct {
	ID     MatchKeyField `json:"id"`
	NUMBER MatchKeyField `json:"number"`
	DATE   MatchKeyField `json:"date"`

	CUSTOMERID MatchKeyField `json:"customerId"`

	SUBTOTAL MatchKeyField `json:"subtotal"`
	SHIPPING MatchKeyField `json:"shipping"`
	DISCOUNT MatchKeyField `json:"discount"`
	TAX      MatchKeyField `json:"tax"`
	TOTAL    MatchKeyField `json:"total"`
	CHANNEL  MatchKeyField `json:"channel"`
}

type OrderERR struct {
	ID         int `json:"ID"`
	Number     int `json:"Number"`
	Date       int `json:"Date"`
	CustomerID int `json:"CustomerID"`
	SubTotal   int `json:"SubTotal"`
	Shipping   int `json:"Shipping"`
	Discount   int `json:"Tax"`
	Tax        int `json:"Discount"`
	Total      int `json:"Total"`
	Channel    int `json:"Channel"`
}

type NER struct {
	FAC       float64 `json:"FAC"`
	GPE       float64 `json:"GPE"`
	LOC       float64 `json:"LOC"`
	NORP      float64 `json:"NORP"`
	ORG       float64 `json:"ORG"`
	PERSON    float64 `json:"PERSON"`
	PRODUCT   float64 `json:"PRODUCT"`
	EVENT     float64 `json:"EVENT"`
	WORKOFART float64 `json:"WORK_OF_ART"`
	LAW       float64 `json:"LAW"`
	LANGUAGE  float64 `json:"LANGUAGE"`
	DATE      float64 `json:"DATE"`
	TIME      float64 `json:"TIME"`
	PERCENT   float64 `json:"PERCENT"`
	MONEY     float64 `json:"MONEY"`
	QUANTITY  float64 `json:"QUANTITY"`
	ORDINAL   float64 `json:"ORDINAL"`
	CARDINAL  float64 `json:"CARDINAL"`
}

var ProjectID = os.Getenv("PROJECTID")
var PubSubTopic = os.Getenv("PSOUTPUT")
var PubSubTopic2 = os.Getenv("PSOUTPUT2")

var ps *pubsub.Client
var topic *pubsub.Topic
var topic2 *pubsub.Topic

func init() {
	ctx := context.Background()
	ps, _ = pubsub.NewClient(ctx, ProjectID)
	topic = ps.Topic(PubSubTopic)
	topic2 = ps.Topic(PubSubTopic2)

	log.Printf("init completed, pubsub topic name: %v", topic)
}

func PostProcessOrder(ctx context.Context, m PubSubMessage) error {
	var input Input
	if err := json.Unmarshal(m.Data, &input); err != nil {
		log.Fatalf("Unable to unmarshal message %v with error %v", string(m.Data), err)
	}

	var mkOutput OrderOutput
	for index, column := range input.Columns {
		if column.OrderERR.ID == 1 {
			matchKey := "ID"
			if len(GetMkField(&mkOutput, matchKey).Value) == 0 {
				SetMkField(&mkOutput, matchKey, column.Value, column.Name)
			}
		}
		if column.OrderERR.Number == 1 {
			matchKey := "NUMBER"
			if len(GetMkField(&mkOutput, matchKey).Value) == 0 {
				SetMkField(&mkOutput, matchKey, column.Value, column.Name)
			}
		}
		if column.OrderERR.CustomerID == 1 {
			matchKey := "CUSTOMERID"
			if len(GetMkField(&mkOutput, matchKey).Value) == 0 {
				SetMkField(&mkOutput, matchKey, column.Value, column.Name)
			}
		}

		if column.OrderERR.Date == 1 {
			matchKey := "DATE"
			if len(GetMkField(&mkOutput, matchKey).Value) == 0 {
				SetMkField(&mkOutput, matchKey, column.Value, column.Name)
			}
		}
		if column.OrderERR.SubTotal == 1 {
			matchKey := "SUBTOTAL"
			if len(GetMkField(&mkOutput, matchKey).Value) == 0 {
				SetMkField(&mkOutput, matchKey, column.Value, column.Name)
			}
		}
		if column.OrderERR.Discount == 1 {
			matchKey := "DISCOUNT"
			if len(GetMkField(&mkOutput, matchKey).Value) == 0 {
				SetMkField(&mkOutput, matchKey, column.Value, column.Name)
			}
		}
		if column.OrderERR.Shipping == 1 {
			matchKey := "SHIPPING"
			if len(GetMkField(&mkOutput, matchKey).Value) == 0 {
				SetMkField(&mkOutput, matchKey, column.Value, column.Name)
			}
		}
		if column.OrderERR.Tax == 1 {
			matchKey := "TAX"
			if len(GetMkField(&mkOutput, matchKey).Value) == 0 {
				SetMkField(&mkOutput, matchKey, column.Value, column.Name)
			}
		}
		if column.OrderERR.Total == 1 {
			matchKey := "TOTAL"
			if len(GetMkField(&mkOutput, matchKey).Value) == 0 {
				SetMkField(&mkOutput, matchKey, column.Value, column.Name)
			}
		}
		if column.OrderERR.Channel == 1 {
			matchKey := "CHANNEL"
			if len(GetMkField(&mkOutput, matchKey).Value) == 0 {
				SetMkField(&mkOutput, matchKey, column.Value, column.Name)
			}
		}
		input.Columns[index] = column
	}

	// pub the record
	var output Output
	output.Signature = input.Signature
	output.Passthrough = input.Passthrough
	output.MatchKeys = mkOutput

	// push into pubsub
	outputJSON, _ := json.Marshal(output)
	psresult := topic.Publish(ctx, &pubsub.Message{
		Data: outputJSON,
		Attributes: map[string]string{
			"type":   "order",
			"source": "post",
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
			"type":   "order",
			"source": "post",
		},
	})

	return nil
}

func GetMkField(v *OrderOutput, field string) MatchKeyField {
	r := reflect.ValueOf(v)
	f := reflect.Indirect(r).FieldByName(field)
	return f.Interface().(MatchKeyField)
}

func SetMkField(v *OrderOutput, field string, value string, source string) string {
	r := reflect.ValueOf(v)
	f := reflect.Indirect(r).FieldByName(field)

	f.Set(reflect.ValueOf(MatchKeyField{Value: value, Source: source}))
	return value
}
