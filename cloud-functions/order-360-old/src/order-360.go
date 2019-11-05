package order360

import (
	"context"
	"encoding/json"
	"log"
	"strconv"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/pubsub"
	"github.com/google/uuid"
)

var (
	pubsubTopic = "order-output-dev"    //"streamer-output-prod"
	outputTopic = "order360-output-dev" // "order-output-prod"
	projectID   = "wemade-core"
)

// OPOutputTrustedID trusted id
type OPOutputTrustedID struct {
	Source   string `json:"Source"`
	SourceID string `json:"SourceId"`
}

// OPConsignment detail
type OPConsignment struct {
	OrderID      string `json:"OrderId" bigquery:"orderid"`
	ConsigmentID string `json:"ConsigmentId" bigquery:"consigmentid"`
	ShipDate     string `json:"ShipDate" bigquery:"shipdate"`
}

// OPDetail of the order
type OPDetail struct {
	DetailID     string `json:"DetailId" bigquery:"detailid"`
	OrderID      string `json:"OrderId" bigquery:"orderid"`
	ConsigmentID string `json:"ConsigmentId" bigquery:"consigmentid"`
	ProductID    string `json:"ProductId" bigquery:"productid"`
	SKU          string `json:"SKU" bigquery:"sku"`
	UPC          string `json:"UPC" bigquery:"upc"`
}

// OPRecord order record
type OPRecord struct {
	TrustedID   string        `json:"TrustedId" bigquery:"trustedid"`
	OrderID     string        `json:"OrderId" bigquery:"orderid"`
	OrderNumber string        `json:"OrderNumber" bigquery:"ordernumber"`
	CustomerID  string        `json:"CustomerId" bigquery:"customerid"`
	OrderDate   string        `json:"OrderDate" bigquery:"orderdate"`
	BillTo      string        `json:"BillTo" bigquery:"billto"`
	Terms       string        `json:"Terms" bigquery:"terms"`
	Total       string        `json:"Total" bigquery:"total"`
	Consignment OPConsignment `json:"Consigment" bigquery:"consigment"`
	Detail      OPDetail      `json:"Detail" bigquery:"detail"`
}

// OPOutputRecord pipeline record struct
type OPOutputRecord struct {
	TrustedID []OPOutputTrustedID `json:"TrustedId"`
	Owner     int64               `json:"Owner"`
	Source    string              `json:"Source"`
	Request   string              `json:"Request"`
	Row       int                 `json:"Row"`
	TimeStamp string              `json:"TimeStamp"`
	Order     OPRecord            `json:"Record" bigquery:"order"`
}

// O3OutputRecord output record to bq
type O3OutputRecord struct {
	Order     OPRecord `json:"Order" bigquery:"order"`
	Owner     int64    `json:"owner" bigquery:"owner"`
	Source    string   `json:"source" bigquery:"source"`
	Request   string   `json:"request" bigquery:"request"`
	Row       int      `json:"row" bigquery:"row"`
	TimeStamp string   `json:"timestamp" bigquery:"timestamp"`
	SetID     string   `json:"set" bigquery:"set"`
}

// PubSubMessage pubsub
type PubSubMessage struct {
	Data []byte `json:"data"`
}

var indexName = "order"

// Order360 round and around goes the order
func Order360(ctx context.Context, m PubSubMessage) error {
	log.Println(string(m.Data))
	var input OPOutputRecord
	if err := json.Unmarshal(m.Data, &input); err != nil {
		log.Fatal(err)
	}

	psclient, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("Could not create pubsub Client: %v", err)
		return nil
	}
	pstopic := psclient.Topic(outputTopic)
	log.Printf("pubsub topic is %v", pstopic)

	// map the data
	var output O3OutputRecord
	output.Owner = input.Owner
	output.Source = input.Source
	output.Request = input.Request
	output.Row = input.Row
	output.TimeStamp = input.TimeStamp
	output.SetID = uuid.New().String()

	output.Order = input.Order

	// write to BQ
	bqClient, err := bigquery.NewClient(ctx, projectID)

	orderSchema, err := bigquery.InferSchema(O3OutputRecord{})
	orderMetaData := &bigquery.TableMetadata{
		Schema: orderSchema,
	}
	datasetID := strconv.FormatInt(input.Owner, 10)
	tableID := "order"
	orderTableRef := bqClient.Dataset(datasetID).Table(tableID)
	if err := orderTableRef.Create(ctx, orderMetaData); err != nil {
		log.Fatalf("error making table %v", err)
		return nil
	}

	orderInserter := orderTableRef.Inserter()
	if err := orderInserter.Put(ctx, output); err != nil {
		log.Fatalf("error insertinng into table %v", err)
		return nil
	}

	log.Printf("%v %v", bqClient, orderSchema)

	outputJSON, err := json.Marshal(output)
	if err != nil {
		log.Fatalf("Could not parse output data: %v", err)
		return nil
	}

	log.Printf("output message %v", string(outputJSON))

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
