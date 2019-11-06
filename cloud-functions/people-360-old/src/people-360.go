package people360

import (
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"log"
	"strconv"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/pubsub"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/google/uuid"
)

// we store things in 3 tables -- fiber, set and people

type PPOutputAddress struct {
	Add1                string  `json:"Add1"`
	Add2                string  `json:"Add2"`
	City                string  `json:"City"`
	Country             string  `json:"Country"`
	DMA                 string  `json:"DMA"`
	Directional         string  `json:"Directional"`
	Lat                 float64 `json:"Lat"`
	Long                float64 `json:"Long"`
	MailRoute           string  `json:"MailRoute"`
	Number              string  `json:"Number"`
	OccupancyIdentifier string  `json:"OccupancyIdentifier"`
	OccupancyType       string  `json:"OccupancyType"`
	PostType            string  `json:"PostType"`
	Postal              string  `json:"Postal"`
	State               string  `json:"State"`
	StreetName          string  `json:"StreetName"`
}

type PPOutputBackground struct {
	Age    int    `json:"Age"`
	DOB    string `json:"DOB"`
	Gender string `json:"Gender"`
	Race   string `json:"Race"`
}

type PPOutputEmail struct {
	Address   string `json:"Address"`
	Confirmed bool   `json:"Confirmed"`
	Domain    string `json:"Domain"`
	Type      string `json:"Type"`
}

type PPOutputName struct {
	First      string `json:"First"`
	Full       string `json:"Full"`
	Last       string `json:"Last"`
	Middle     string `json:"Middle"`
	Nick       string `json:"Nick"`
	Salutation string `json:"Salutation"`
	Suffix     string `json:"Suffix"`
}

type PPOutputOrganization struct {
	Location string `json:"Location"`
	Name     string `json:"Name"`
	Role     string `json:"Role"`
	SIC      string `json:"SIC"`
	Status   string `json:"Status"`
	Title    string `json:"Title"`
}

type PPOutputPhone struct {
	Area      string `json:"Area"`
	Confirmed bool   `json:"Confirmed"`
	Country   string `json:"Country"`
	Exchange  string `json:"Exchange"`
	Provider  string `json:"Provider"`
	Station   string `json:"Station"`
	Type      string `json:"Type"`
	Number    string `json:"Number"`
}

type PPOutputTrustedID struct {
	Source   string `json:"Source"`
	SourceID string `json:"SourceId"`
}

type PPOutputRecord struct {
	Address      []PPOutputAddress      `json:"Address"`
	Background   PPOutputBackground     `json:"Background"`
	Email        []PPOutputEmail        `json:"Email"`
	Name         PPOutputName           `json:"Name"`
	Organization []PPOutputOrganization `json:"Organization"`
	Phone        []PPOutputPhone        `json:"Phone"`
	TrustedID    []PPOutputTrustedID    `json:"TrustedId"`
	Owner        int64                  `json:"Owner"`
	Source       string                 `json:"Source"`
	Request      string                 `json:"Request"`
	Row          int                    `json:"Row"`
	TimeStamp    string                 `json:"TimeStamp"`
}

type P3Fiber struct {
	FiberID   string `json:"fiber" bigquery:"id"`
	Owner     int64  `json:"owner" bigquery:"owner"`
	Source    string `json:"source" bigquery:"source"`
	Request   string `json:"request" bigquery:"request"`
	Row       int    `json:"row" bigquery:"row"`
	TimeStamp string `json:"timestamp" bigquery:"timestamp"`

	Address      []PPOutputAddress      `json:"addresses" bigquery:"addresses"`
	Background   PPOutputBackground     `json:"background" bigquery:"background"`
	Email        []PPOutputEmail        `json:"emails" bigquery:"emails"`
	Name         PPOutputName           `json:"name" bigquery:"name"`
	Organization []PPOutputOrganization `json:"organization" bigquery:"organization"`
	Phone        []PPOutputPhone        `json:"phones" bigquery:"phones"`
	TrustedID    []PPOutputTrustedID    `json:"trustedId" bigquery:"trustedId"`
}

type P3OutputAddress struct {
	Add1   string `json:"add1" bigquery:"add1"`
	Add2   string `json:"add2" bigquery:"add2"`
	City   string `json:"city" bigquery:"city"`
	Postal string `json:"postal" bigquery:"postal"`
	State  string `json:"state" bigquery:"state"`
}

type P3OutputBackground struct {
	Gender string `json:"gender" bigquery:"gender"`
}

type P3OutputEmail struct {
	Address string `json:"address" bigquery:"address"`
}

type P3OutputName struct {
	First string `json:"first" bigquery:"first"`
	Full  string `json:"full" bigquery:"full"`
	Last  string `json:"last" bigquery:"last"`
}

type P3OutputOrganization struct {
	Role  string `json:"role" bigquery:"role"`
	Title string `json:"title" bigquery:"title"`
	Name  string `json:"name" bigquery:"name"`
}

type P3OutputPhone struct {
	Phone string `json:"phone" json:"phone"`
}

type P3Set struct {
	SetID        string   `json:"set" bigquery:"id"`
	FiberID      []string `json:"fibers" bigquery:"fibers"`
	TrustedID    []string `json:"trustedId" bigquery:"trustedID"`
	Email        []string `json:"email" bigquery:"email"`
	Phone        []string `json:"phone" bigquery:"phone"`
	FirstName    []string `json:"firstName" bigquery:"firstName"`
	FirstInitial []string `json:"firstInitial" bigquery:"firstInitial"`
	LastName     []string `json:"lastName" bigquery:"lastName"`
	City         []string `json:"city" bigquery:"city"`
	State        []string `json:"state" bigquery:"state"`
	Zip5         []string `json:"zip5" bigquery:"zip5"`
	StreetNumber []string `json:"streetNumber" bigquery:"streetNumber"`
	StreetName   []string `json:"streetName" bigquery:"streetName"`
}

type PubSubMessage struct {
	Data []byte `json:"data"`
}

var indexName = "people"
var ProjectID = "wemade-core"
var bqClient bigquery.Client
var fiberSchema bigquery.Schema
var setSchema bigquery.Schema

func init() {

}

func People360(ctx context.Context, m PubSubMessage) error {
	log.Println(string(m.Data))
	var input PPOutputRecord
	if err := json.Unmarshal(m.Data, &input); err != nil {
		log.Fatal(err)
	}

	cfg := elasticsearch.Config{
		Addresses: []string{
			"http://104.198.136.122:9200",
		},
		Username: "elastic",
		Password: "TsLv8BtM",
	}
	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		log.Fatalf("Error creating the client: %s", err)
	}

	PubsubTopic := "people360-output-dev"

	bqClient, _ := bigquery.NewClient(ctx, ProjectID)
	log.Print("init created bqClient %v", bqClient.Location)
	// store the fiber
	fiberSchema, _ := bigquery.InferSchema(P3Fiber{})
	log.Print("init created fiber schema %v", fiberSchema)

	setSchema, _ := bigquery.InferSchema(P3Set{})
	log.Print("init created set schema %v", setSchema)

	psclient, err := pubsub.NewClient(ctx, ProjectID)
	if err != nil {
		log.Fatalf("Could not create pubsub Client: %v", err)
		return nil
	}
	pstopic := psclient.Topic(PubsubTopic)
	log.Printf("pubsub topic is %v", pstopic)

	docID := uuid.New().String() //strconv.FormatInt(input.Owner, 10) + "-" + input.Source + "-" + input.Request + "-" + strconv.Itoa(input.Row)

	// dupeRequest := esapi.GetRequest{
	// 	Index:        indexName,
	// 	DocumentType: "record",
	// 	DocumentID:   docID,
	// }
	// dupeRes, err := dupeRequest.Do(ctx, es)
	// if err != nil {
	// 	log.Fatalf("Error getting response: %s", err)
	// }
	// defer dupeRes.Body.Close()

	// if dupeRes.IsError() {
	// 	resB, _ := ioutil.ReadAll(dupeRes.Body)
	// 	log.Printf("[%s] Error looking up document ID=%v, Message=%v", dupeRes.Status(), docID, string(resB))
	// } else {
	// 	resB, _ := ioutil.ReadAll(dupeRes.Body)
	// 	log.Printf("[%s] document ID=%v, Message=%v", dupeRes.Status(), docID, string(resB))
	// }

	req := esapi.IndexRequest{
		Index:        indexName,
		DocumentType: "record", //input.Request,
		DocumentID:   docID,
		Body:         bytes.NewReader(m.Data),
		Refresh:      "true",
	}
	res, err := req.Do(ctx, es)
	if err != nil {
		log.Fatalf("Error getting response: %s", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		resB, _ := ioutil.ReadAll(res.Body)
		log.Printf("[%s] Error indexing document ID=%v, Message=%v", res.Status(), docID, string(resB))
	} else {
		resB, _ := ioutil.ReadAll(res.Body)
		log.Printf("[%s] document ID=%v, Message=%v", res.Status(), docID, string(resB))
	}

	// map the data
	var output P3Fiber
	output.Owner = input.Owner
	output.Source = input.Source
	output.Request = input.Request
	output.Request = input.Request
	output.Row = input.Row
	output.TimeStamp = input.TimeStamp
	output.FiberID = uuid.New().String()
	output.Address = input.Address
	output.Email = input.Email
	output.Name = input.Name
	output.Phone = input.Phone
	output.Organization = input.Organization
	output.Background = input.Background
	output.TrustedID = input.TrustedID

	// check for

	// write to BQ
	fiberMeta := &bigquery.TableMetadata{
		Schema: fiberSchema,
	}

	setMeta := &bigquery.TableMetadata{
		Schema: setSchema,
	}

	datasetID := strconv.FormatInt(input.Owner, 10)

	// make sure dataset exists
	dsmeta := &bigquery.DatasetMetadata{
		Location: "US", // Create the dataset in the US.
	}
	if err := bqClient.Dataset(datasetID).Create(ctx, dsmeta); err != nil {
	}

	fiberTableID := "people_fiber"
	fiberTable := bqClient.Dataset(datasetID).Table(fiberTableID)
	if err := fiberTable.Create(ctx, fiberMeta); err != nil {
		// log.Fatalf("error making table %v", err)
		// return nil
	}

	setTableID := "people_set"
	setTable := bqClient.Dataset(datasetID).Table(setTableID)
	if err := setTable.Create(ctx, setMeta); err != nil {
		// log.Fatalf("error making table %v", err)
		// return nil
	}

	fiberInserter := fiberTable.Inserter()
	if err := fiberInserter.Put(ctx, output); err != nil {
		log.Fatalf("error insertinng into fiber table %v", err)
		return nil
	}

	// new set
	var set P3Set
	set.FiberID = []string{output.FiberID}
	set.SetID = uuid.New().String()

	if len(output.TrustedID) > 0 {
		for _, TrustedID := range output.TrustedID {
			set.TrustedID = append(set.TrustedID, TrustedID.SourceID)
		}
	}
	if len(output.Email) > 0 {
		for _, Email := range output.Email {
			set.Email = append(set.Email, Email.Address)
		}
	}
	if len(output.Phone) > 0 {
		for _, Phone := range output.Phone {
			set.Phone = append(set.Phone, Phone.Number)
		}
	}
	if len(output.Name.First) > 0 {
		set.FirstName = append(set.FirstName, output.Name.First)
		set.FirstInitial = append(set.FirstInitial, output.Name.First[0:1])
	}
	if len(output.Name.Last) > 0 {
		set.LastName = append(set.LastName, output.Name.Last)
	}

	if len(output.Address) > 0 {
		for _, Address := range output.Address {
			set.City = append(set.City, Address.City)
			set.State = append(set.State, Address.State)
			if len(Address.Postal) > 5 {
				set.Zip5 = append(set.Zip5, Address.Postal[0:5])
			}
			set.StreetNumber = append(set.StreetNumber, Address.Number)
			set.StreetName = append(set.StreetName, Address.StreetName)
		}
	}

	setInserter := setTable.Inserter()
	if err := setInserter.Put(ctx, set); err != nil {
		log.Fatalf("error insertinng into set table %v", err)
		return nil
	}

	return nil
}
