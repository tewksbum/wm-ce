package orderpipeline

import (
	"context"
	"encoding/json"
	"log"

	"cloud.google.com/go/pubsub"
)

// PubSubMessage is the payload of a pubsub event
type PubSubMessage struct {
	Data []byte `json:"data"`
}

var (
	pubsubTopic = "streamer-output-dev" //"streamer-output-prod"
	outputTopic = "order-output-dev"    // "order-output-prod"
	projectID   = "wemade-core"
)

// IdentifiedRecord struct
type IdentifiedRecord struct {
	TrustedID   string      `json:"TrustedId" bigquery:"trustedid"`
	OrderID     string      `json:"OrderId" bigquery:"orderid"`
	OrderNumber string      `json:"OrderNumber" bigquery:"ordernumber"`
	CustomerID  string      `json:"CustomerId" bigquery:"customerid"`
	OrderDate   string      `json:"OrderDate" bigquery:"orderdate"`
	BillTo      string      `json:"BillTo" bigquery:"billto"`
	Terms       string      `json:"Terms" bigquery:"terms"`
	Total       string      `json:"Total" bigquery:"total"`
	Consignment Consignment `json:"Consigment" bigquery:"consigment"`
	Detail      Detail      `json:"Detail" bigquery:"detail"`
}

// InputERR struct defining the input columns
type InputERR struct {
	//  Trusted ID
	TrustedID int      `json:"TrustedID"`
	Order     OrderERR `json:"Order"`
}

// InputVER value regex
type InputVER struct {
	Hashcode  int64
	IsOrderID bool `json:"isOrderID"`
}

// InputColumn input column
type InputColumn struct {
	ERR   InputERR `json:"ERR"`
	VER   InputVER `json:"VER"`
	Name  string   `json:"Name"`
	Value string   `json:"Value"`
}

// InputRecord the input record
type InputRecord struct {
	Columns   []InputColumn `json:"Columns"`
	Owner     int64         `json:"Owner"`
	Request   string        `json:"Request"`
	Row       int           `json:"Row"`
	Source    string        `json:"Source"`
	TimeStamp string        `json:"TimeStamp"`
}

// OutputTrustedID Trusted ID
type OutputTrustedID struct {
	Source   string `json:"Source"`
	SourceID string `json:"SourceId"`
}

// OutputRecord the output result
type OutputRecord struct {
	TrustedID []OutputTrustedID `json:"TrustedId"`
	Owner     int64             `json:"Owner"`
	Source    string            `json:"Source"`
	Request   string            `json:"Request"`
	Row       int               `json:"Row"`
	TimeStamp string            `json:"TimeStamp"`
	Record    IdentifiedRecord  `json:"Record" bigquery:"Order"`
}

func pipelineParse(input InputRecord) (output *OutputRecord, err error) {
	var mkOutput IdentifiedRecord

	if err = parseOrderHeader(&input, &mkOutput); err != nil {
		return nil, err
	}
	if err = parseOrderConsignment(&input, &mkOutput); err != nil {
		return nil, err
	}
	if err = parseOrderDetail(&input, &mkOutput); err != nil {
		return nil, err
	}

	// assemble output
	output = new(OutputRecord)
	if mkOutput.TrustedID != "" {
		outputTrustedID := OutputTrustedID{
			Source:   input.Source,
			SourceID: mkOutput.TrustedID,
		}
		output.TrustedID = append(output.TrustedID, outputTrustedID)
	}
	output.Owner = input.Owner
	output.Request = input.Request
	output.Source = input.Source
	output.Row = input.Row
	output.TimeStamp = input.TimeStamp
	output.Record = mkOutput

	mkJSON, _ := json.Marshal(mkOutput)
	log.Printf("MatchKey Columns after Clean up %v", string(mkJSON))

	return output, err
}

// Main func for pipeline
func Main(ctx context.Context, m PubSubMessage) error {
	var input InputRecord
	if err := json.Unmarshal(m.Data, &input); err != nil {
		log.Fatal(err)
		return nil
	}

	output, err := pipelineParse(input)
	if err != nil {
		log.Fatalf("Could not parse input data: %v", err)
		return nil
	}

	// if writeToBQ(strconv.FormatInt(input.Owner, 10), "orders", output) != nil {
	// 	log.Fatalf("Could not store to bigquery: %v", err)
	// 	return nil
	// }

	outputJSON, err := json.Marshal(output)
	if err != nil {
		log.Fatalf("Could not parse output data: %v", err)
		return nil
	}

	log.Printf("output message %v", string(outputJSON))

	psclient, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("Could not create pubsub Client: %v", err)
		return nil
	}
	pstopic := psclient.Topic(outputTopic)
	psresult := pstopic.Publish(ctx, &pubsub.Message{
		Data: outputJSON,
	})
	psid, err := psresult.Get(ctx)
	if err != nil {
		log.Fatalf("Could not pub to pubsub: %v", err)
	} else {
		log.Printf("Published record message id %v", psid)
	}

	return nil
}
