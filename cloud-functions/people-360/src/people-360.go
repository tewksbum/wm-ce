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

type P3OutputRecord struct {
	Address      []P3OutputAddress    `json:"addresses" bigquery:"addresses"`
	Background   P3OutputBackground   `json:"background" bigquery:"background"`
	Email        []P3OutputEmail      `json:"emails" bigquery:"emails"`
	Name         P3OutputName         `json:"name" bigquery:"name"`
	Organization P3OutputOrganization `json:"organization" bigquery:"organization"`
	Phone        []P3OutputPhone      `json:"phones" bigquery:"phones"`
	Owner        int64                `json:"owner" bigquery:"owner"`
	Source       string               `json:"source" bigquery:"source"`
	Request      string               `json:"request" bigquery:"request"`
	Row          int                  `json:"row" bigquery:"row"`
	TimeStamp    string               `json:"timestamp" bigquery:"timestamp"`
	SetID        string               `json:"set" bigquery:"set"`
}

type PubSubMessage struct {
	Data []byte `json:"data"`
}

var indexName = "people"

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
	ProjectID := "wemade-core"

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
	var output P3OutputRecord
	output.Owner = input.Owner
	output.Source = input.Source
	output.Request = input.Request
	output.Request = input.Request
	output.Row = input.Row
	output.TimeStamp = input.TimeStamp
	output.SetID = uuid.New().String()

	for _, inputEmail := range input.Email {
		outputEmail := P3OutputEmail{
			Address: inputEmail.Address,
		}
		output.Email = append(output.Email, outputEmail)
	}

	for _, inputAddress := range input.Address {
		outputAddress := P3OutputAddress{
			Add1:   inputAddress.Add1,
			Add2:   inputAddress.Add2,
			City:   inputAddress.City,
			State:  inputAddress.State,
			Postal: inputAddress.Postal,
		}
		output.Address = append(output.Address, outputAddress)
	}

	for _, inputPhone := range input.Phone {
		outputPhone := P3OutputPhone{
			Phone: inputPhone.Number,
		}
		output.Phone = append(output.Phone, outputPhone)
	}
	// Background   P3OutputBackground   `json:"background"`
	// Name         P3OutputName         `json:"name"`
	// Organization P3OutputOrganization `json:"organization"`
	// Phone        []P3OutputPhone      `json:"phones"`

	// write to BQ
	bqClient, err := bigquery.NewClient(ctx, ProjectID)

	peopleSchema, err := bigquery.InferSchema(P3OutputRecord{})
	peopleMetaData := &bigquery.TableMetadata{
		Schema: peopleSchema,
	}
	datasetID := strconv.FormatInt(input.Owner, 10)
	tableID := "people"
	peopleTableRef := bqClient.Dataset(datasetID).Table(tableID)
	if err := peopleTableRef.Create(ctx, peopleMetaData); err != nil {
		log.Fatalf("error making table %v", err)
		return nil
	}

	peopleInserter := peopleTableRef.Inserter()
	if err := peopleInserter.Put(ctx, output); err != nil {
		log.Fatalf("error insertinng into table %v", err)
		return nil
	}

	log.Printf("%v %v", bqClient, peopleSchema)
	return nil
}
