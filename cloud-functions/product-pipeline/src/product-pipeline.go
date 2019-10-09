package productpipeline

import (
	"context"
	"encoding/json"
	"log"
	"strconv"

	"cloud.google.com/go/bigquery"

	"cloud.google.com/go/pubsub"
)

// PubSubMessage is the payload of a pubsub event
type PubSubMessage struct {
	Data []byte `json:"data"`
}

var (
	pubsubTopic = "streamer-output-dev" //"streamer-output-prod"
	outputTopic = "product-output-dev"  // "product-output-prod"
	projectID   = "wemade-core"
)

// IdentifiedRecord struct
type IdentifiedRecord struct {
	PID         string   `json:"PID" bigquery:"pid"`
	SKU         string   `json:"SKU" bigquery:"sku"`
	UPC         string   `json:"UPC" bigquery:"upc"`
	Name        string   `json:"Name" bigquery:"name"`
	Description string   `json:"Description" bigquery:"description"`
	Size        string   `json:"Size" bigquery:"size"`
	Color       string   `json:"Color" bigquery:"color"`
	UnitPrice   string   `json:"UnitPrice" bigquery:"unitprice"`
	Contains    []string `json:"Contains" bigquery:"contains"`
	Type        string   `json:"Type" bigquery:"type"`
	VendorID    string   `json:"VendorId" bigquery:"vendorid"`
	Vendor      string   `json:"Vendor" bigquery:"vendor"`
	Stars       string   `json:"Stars" bigquery:"stars"`
	Category    string   `json:"Category" bigquery:"category"`
}

// InputERR struct defining the input columns
type InputERR struct {
	TrustedID   int `json:"TrustedID"`
	ProductID   int `json:"ProductId"`
	SKU         int `json:"ProductSKU"`
	UPC         int `json:"ProductUPC"`
	Name        int `json:"ProductName"`
	Description int `json:"ProductDescription"`
	Size        int `json:"ProductSize"`
	Color       int `json:"ProductColor"`
	UnitPrice   int `json:"ProductUnitPrice"`
	Contains    int `json:"ProductContains"`
	Type        int `json:"ProductType"`
	VendorID    int `json:"ProductVendorId"`
	Vendor      int `json:"ProductVendor"`
	Stars       int `json:"ProductStars"`
	Category    int `json:"ProductCategory"`
}

// InputColumn input column
type InputColumn struct {
	ERR   InputERR `json:"ERR"`
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
	Record    IdentifiedRecord  `json:"Record" bigquery:"Product"`
}

func init() {

}

func pipelineParse(input InputRecord) (output *OutputRecord, err error) {
	var mkOutput IdentifiedRecord
	var trustedID string

	for _, column := range input.Columns {
		if column.ERR.TrustedID == 1 {
			trustedID = column.Value
		}
		if column.ERR.ProductID == 1 {
			mkOutput.PID = column.Value
		}
		if column.ERR.Category == 1 {
			mkOutput.Category = column.Value
		}
		if column.ERR.Color == 1 {
			mkOutput.Color = column.Value
		}
		if column.ERR.Description == 1 {
			mkOutput.Description = column.Value
		}
		if column.ERR.Name == 1 {
			mkOutput.Name = column.Value
		}
		if column.ERR.SKU == 1 {
			mkOutput.SKU = column.Value
		}
		if column.ERR.Size == 1 {
			mkOutput.Size = column.Value
		}
		if column.ERR.Stars == 1 {
			mkOutput.Stars = column.Value
		}
		if column.ERR.Type == 1 {
			mkOutput.Type = column.Value
		}
		if column.ERR.UnitPrice == 1 {
			mkOutput.UnitPrice = column.Value
		}
		if column.ERR.Vendor == 1 {
			mkOutput.Vendor = column.Value
		}
		if column.ERR.VendorID == 1 {
			mkOutput.VendorID = column.Value
		}
	}

	// assemble output
	output = new(OutputRecord)
	if trustedID != "" {
		outputTrustedID := OutputTrustedID{
			Source:   input.Source,
			SourceID: trustedID,
		}
		output.TrustedID = append(output.TrustedID, outputTrustedID)
	}
	output.Owner = input.Owner
	output.Request = input.Request
	output.Source = input.Source
	output.Row = input.Row
	output.TimeStamp = input.TimeStamp
	output.Record = mkOutput

	// mkJSON, _ := json.Marshal(mkOutput)
	// log.Printf("MatchKey Columns after Clean up %v", string(mkJSON))

	return output, err
}

func writeToBQ(datasetID string, tableID string, output *OutputRecord) error {
	// Write record to BQ
	ctx := context.Background()
	bqClient, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return err
	}
	recordSchema, err := bigquery.InferSchema(output)
	recordMetaData := &bigquery.TableMetadata{
		Schema: recordSchema,
	}
	recordTableRef := bqClient.Dataset(datasetID).Table(tableID)
	if err := recordTableRef.Create(ctx, recordMetaData); err != nil {
		return err
	}
	recordInserter := recordTableRef.Inserter()
	if err := recordInserter.Put(ctx, output); err != nil {
		return err
	}
	return nil
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

	if writeToBQ(strconv.FormatInt(input.Owner, 10), "products", output) != nil {
		log.Fatalf("Could not store to bigquery: %v", err)
		return nil
	}

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
