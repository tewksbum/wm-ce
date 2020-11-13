package orderevent

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"time"

	"cloud.google.com/go/pubsub"
)

// PubSubMessage is the payload of a pubsub event
type PubSubMessage struct {
	Data []byte `json:"data"`
}

// orders is the payload in order-intake pubsub
type orders []struct {
	ID          int64    `json:"id"`
	OrderNumber string `json:"orderNumber"`
	Type        string `json:"type"`
	Status      string `json:"status"`
	Totals      struct {
		MerchandiseCostTotal float64 `json:"merchandiseCostTotal"`
		MerchandiseTotal     float64     `json:"merchandiseTotal"`
		MerchandiseTaxTotal  float64 `json:"merchandiseTaxTotal"`
		ShippingTotal        float64     `json:"shippingTotal"`
		ShippingTaxTotal     float64 `json:"shippingTaxTotal"`
		DiscountTotal        float64     `json:"discountTotal"`
		ServiceTotal         float64     `json:"serviceTotal"`
		ServiceTaxTotal      float64     `json:"serviceTaxTotal"`
		Total                float64 `json:"total"`
	} `json:"totals"`
	Dates struct {
		PlacedOn  time.Time `json:"placedOn"`
		CreatedOn time.Time `json:"createdOn"`
		UpdatedOn time.Time `json:"updatedOn"`
	} `json:"dates"`
	Attributes struct {
		WebOrderNumber string `json:"webOrderNumber"`
		WebOrderID     int64    `json:"webOrderId"`
		Subsidiary     string `json:"subsidiary"`
		Channel        string `json:"channel"`
		ChannelID      int64    `json:"channelId"`
		Source         string `json:"source"`
		SourceID       int64    `json:"sourceId"`
		School         string `json:"school"`
		SchoolID       int64    `json:"schoolId"`
	} `json:"attributes"`
	Customer struct {
		ID   int    `json:"id"`
		Name string `json:"name"`
	} `json:"customer"`
	Billing struct {
		AddressKey string `json:"addressKey"`
		Country    string `json:"country"`
		Addr1      string `json:"addr1"`
		Addr2      string `json:"addr2"`
		City       string `json:"city"`
		State      string `json:"state"`
		Zip        string `json:"zip"`
		Name       string `json:"name"`
		Phone      string `json:"phone"`
		Email      string `json:"email"`
	} `json:"billing"`
	Fees      []interface{} `json:"fees"`
	Shipments []struct {
		Lines []struct {
			ExtPrice    float64     `json:"extPrice"`
			Quantity    int     `json:"quantity"`
			Lob         string  `json:"lob"`
			Cost        float64 `json:"cost"`
			Type        string  `json:"type"`
			UnitPrice   float64     `json:"unitPrice"`
			Shipment    string  `json:"shipment"`
			ItemTitle   string  `json:"itemTitle"`
			ItemSku     string  `json:"itemSku"`
			ItemID      int64     `json:"itemId"`
			IsDropship  bool    `json:"isDropship"`
			IsCancelled bool    `json:"isCancelled"`
			LineID      string  `json:"lineId"`
			UniqueKey   string  `json:"uniqueKey"`
			Kit         string  `json:"kit"`
			LineNo      int     `json:"lineNo"`
			LineIndex   int     `json:"lineIndex"`
			Discount    float64     `json:"discount"`
			Shipping    float64     `json:"shipping"`
			Tax         float64 `json:"tax"`
		} `json:"lines"`
		AddressKey   string      `json:"addressKey"`
		Addr1        string      `json:"addr1"`
		Addr2        string      `json:"addr2"`
		City         string      `json:"city"`
		State        string      `json:"state"`
		Zip          string      `json:"zip"`
		Country         string      `json:"country"`
		Name         string      `json:"name"`
		Phone        string      `json:"phone"`
		Email        string      `json:"email"`
		Type         string      `json:"type"`
		ProgramID    int64         `json:"programId"`
		ProgramName  string      `json:"programName"`
		SponsorID    int64         `json:"sponsorId"`
		SponsorName  string      `json:"sponsorName"`
		ProductTotal interface{} `json:"productTotal,omitempty"`
	} `json:"shipments"`
	Sponsor1     int    `json:"sponsor_1"`
	Sponsorname1 string `json:"sponsorname_1"`
}

// address is the output
type address struct {
	NetsuiteKey string `json:"netsuite_key"`
	Name string `json:"name"`
	Addr1 string `json:"addr1"`
	Addr2 string `json:"addr2"`
	City string `json:"city"`
	State string `json:"state"`
	Zip string `json:"zip"`
	Country string `json:"country"`
	Phone string `json:"phone"`
	Email string `json:"email"`
	Owner string `json:"owner"`
	Source string `json:"source"`
	EventID string `json:"event_id"`
	EventType string `json:"event_type"`
	AddressType string `json:"address_type"`
}

var ProjectID = os.Getenv("PROJECTID")
var PubSubTopic = os.Getenv("PSOUTPUT")

var ps *pubsub.Client
var topic *pubsub.Topic
var topic2 *pubsub.Topic

func init() {
	ctx := context.Background()
	ps, _ = pubsub.NewClient(ctx, ProjectID)
	topic = ps.Topic(PubSubTopic)

	log.Printf("init completed, pubsub topic name: %v", topic)
}

func ProcessOrder(ctx context.Context, m PubSubMessage) error {
	var input orders
	if err := json.Unmarshal(m.Data, &input); err != nil {
		log.Fatalf("Unable to unmarshal message %v with error %v", string(m.Data), err)
	}

	for _, order := range input {
		if len(order.Billing.AddressKey) > 0 {
			ad := address {
				NetsuiteKey: order.Billing.AddressKey,
				Name: order.Billing.Name,
				Addr1: order.Billing.Addr1,
				Addr2: order.Billing.Addr2,
				City: order.Billing.City,
				State: order.Billing.State,
				Zip: order.Billing.Zip,
				Country: order.Billing.Country,
				Phone: order.Billing.Phone,
				Email: order.Billing.Email,
				Owner: order.Sponsorname1,
				Source:order.Attributes.Channel,
				EventID: order.OrderNumber,
				EventType: "order",
				AddressType: "billto",
			}

			// push into pubsub
			outputJSON, _ := json.Marshal(ad)
			_ = topic.Publish(ctx, &pubsub.Message{
				Data: outputJSON,
				Attributes: map[string]string{
					"type":   "order",
					"source": "post",
				},
			})
		}

		for _, shipment := range order.Shipments {
			ad := address {
				NetsuiteKey: shipment.AddressKey,
				Name: shipment.Name,
				Addr1: shipment.Addr1,
				Addr2: shipment.Addr2,
				City: shipment.City,
				State: shipment.State,
				Zip: shipment.Zip,
				Country: shipment.Country,
				Phone: shipment.Phone,
				Email: shipment.Email,
				Owner: order.Sponsorname1,
				Source: order.Attributes.Channel,
				EventID: order.OrderNumber,
				EventType: "order",
				AddressType: "shipto",
			}

			// push into pubsub
			outputJSON, _ := json.Marshal(ad)
			_ = topic.Publish(ctx, &pubsub.Message{
				Data: outputJSON,
				Attributes: map[string]string{
					"type":   "order",
					"source": "post",
				},
			})
		}
	}

	return nil
}
