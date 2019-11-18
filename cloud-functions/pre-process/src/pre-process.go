package preprocess

import (
	"context"
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"
	"google.golang.org/api/ml/v1"

	"github.com/gomodule/redigo/redis"

	"github.com/xojoc/useragent"
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

type Input struct {
	Signature   Signature         `json:"signature"`
	Passthrough map[string]string `json:"passthrough"`
	Fields      map[string]string `json:"fields"`
	Attributes  map[string]string `json:"attributes"`
}

type Prediction struct {
	Predictions []float64 `json:"predictions"`
}

type MLInput struct {
	Instances [][]float64 `json:"instances"`
}

type CampaignERR struct {
	TrustedID  int `json:"TrustedID"`
	CampaignID int `json:"CampaignId"`
	Name       int `json:"Name"`
	Type       int `json:"Type"`
	Channel    int `json:"Channel"`
	StartDate  int `json:"StartDate"`
	EndDate    int `json:"EndDate"`
	Budget     int `json:"Budget"`
}

type ConsignmentERR struct {
	ID       int `json:"ID"`
	ShipDate int `json:"ShipDate"`
}

type EventERR struct {
	ID         int `json:"ID"`
	Type       int `json:"Type"`
	CampaignID int `json:"CampaignId"`
	Browser    int `json:"Browser"`
	Channel    int `json:"Channel"`
	OS         int `json:"StartDate"`
	Domain     int `json:"Domain"`
	URL        int `json:"URL"`
	Location   int `json:"Location"`
	Referrer   int `json:"Referrer"`
	SearchTerm int `json:"SearchTerm"`
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
}

type OrderDetailERR struct {
	ID              int `json:"ID"`
	OrderID         int `json:"OrderID"`
	OrderNumber     int `json:"OrderNumber"`
	ConsigmentID    int `json:"ConsigmentID"`
	ProductID       int `json:"ProductID"`
	ProductSKU      int `json:"ProductSKU"`
	ProductUPC      int `json:"ProductUPC"`
	ProductQuantity int `json:"ProductQuantity"`
}

type PeopleERR struct {
	Address             int `json:"Address"`
	Address1            int `json:"Address1"`
	Address2            int `json:"Address2"`
	Address3            int `json:"Address3"`
	Age                 int `json:"Age"`
	Birthday            int `json:"Birthday"`
	City                int `json:"City"`
	Country             int `json:"Country"`
	County              int `json:"County"`
	Email               int `json:"Email"`
	FirstName           int `json:"FirstName"`
	FullName            int `json:"FullName"`
	Gender              int `json:"Gender"`
	LastName            int `json:"LastName"`
	MiddleName          int `json:"MiddleName"`
	ParentEmail         int `json:"ParentEmail"`
	ParentFirstName     int `json:"ParentFirstName"`
	ParentLastName      int `json:"ParentLastName"`
	ParentName          int `json:"ParentName"`
	Phone               int `json:"Phone"`
	State               int `json:"State"`
	Suffix              int `json:"Suffix"`
	ZipCode             int `json:"ZipCode"`
	TrustedID           int `json:"TrustedID"`
	Title               int `json:"Title"`
	Role                int `json:"Role"`
	Dorm                int `json:"Dorm"`
	Room                int `json:"Room"`
	Organization        int `json:"Organization"`
	AddressTypeCampus   int `json:"ATCampus"`
	AddressTypeHome     int `json:"ATHome"`
	AddressTypeBilling  int `json:"ATBilling"`
	AddressTypeShipping int `json:"ATShipping"`
}

type ProductERR struct {
	PID         int `json:"ID"`
	SKU         int `json:"SKU"`
	UPC         int `json:"UPC"`
	Name        int `json:"Name"`
	Description int `json:"Description"`
	Size        int `json:"Size"`
	Color       int `json:"Color"`
	UnitPrice   int `json:"UnitPrice"`
	Contains    int `json:"Contains"`
	Type        int `json:"Type"`
	VendorID    int `json:"VendorId"`
	Vendor      int `json:"Vendor"`
	Cost        int `json:"Cost"`
	Stars       int `json:"Stars"`
	Category    int `json:"Category"`
	Margin      int `json:"Margin"`
}

type ERRFlags struct {
	PeopleFirstName      bool
	PeopleLastName       bool
	PeopleAddress        bool
	PeopleAddress1       bool
	PeopleCity  		 bool
	PeopleZip  		     bool
	PeoplePhone  	     bool
	PeopleEmail  	     bool
	PeopleClientID       bool
	ProductID            bool
	ProductSKU           bool
	ProductName          bool
	ProductVendor        bool
	CampaignID           bool
	EventID              bool
	OrderID              bool
	ConsignmentID        bool
	OrderDetailID        bool
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

type PeopleVER struct {
	HASHCODE     int64 `json:"HASH"`
	IS_FIRSTNAME bool  `json:"isFIRSTNAME"`
	IS_LASTNAME  bool  `json:"isLASTNAME"`
	IS_STREET1   bool  `json:"isSTREET1"`
	IS_STREET2   bool  `json:"isSTREET2"`
	IS_STREET3   bool  `json:"isSTREET3"`
	IS_CITY      bool  `json:"isCITY"`
	IS_STATE     bool  `json:"isSTATE"`
	IS_ZIPCODE   bool  `json:"isZIPCODE"`
	IS_COUNTRY   bool  `json:"isCOUNTRY"`
	IS_EMAIL     bool  `json:"isEMAIL"`
	IS_PHONE     bool  `json:"isPHONE"`
}

type EventVER struct {
	IS_BROWSER bool `json:"isBROWSER"`
	IS_CHANNEL bool `json:"isCHANNEL"`
}

type InputColumn struct {
	NER            NER            `json:"NER"`
	PeopleERR      PeopleERR      `json:"PeopleERR"`
	ProductERR     ProductERR     `json:"ProductERR"`
	CampaignERR    CampaignERR    `json:"CampaignERR"`
	OrderERR       OrderERR       `json:"OrderERR"`
	ConsignmentERR ConsignmentERR `json:"ConsignmentERR"`
	OrderDetailERR OrderDetailERR `json:"OrderDetailERR"`
	PeopleVER      PeopleVER      `json:"VER"`
	EventERR       EventERR       `json:"EventERR"`
	EventVER       EventVER       `json:"EventVER"`
	Name           string         `json:"Name"`
	Value          string         `json:"Value"`
	MatchKey       string         `json:"MK"`
}

type CityStateZip struct {
	Cities []string `json:"cities"`
	State  string   `json:"state"`
	Zip    string   `json:"zip"`
}

type NERCache struct {
	Columns      []NERcolumns `json:"columns"`
	TimeStamp    time.Time    `json:"time"`
	ApplyCounter int          `json:"counter"`
	Recompute    bool         `json:"dirty"`
	Source       string       `json:"source`
}

type NERcolumns struct {
	ColumnName  string             `json:"ColumnName"`
	NEREntities map[string]float64 `json:"NEREntities"`
}

type Output struct {
	Signature   Signature         `json:"signature"`
	Passthrough map[string]string `json:"passthrough"`
	Prediction  Prediction        `json:"prediction`
	Columns     []InputColumn     `json:"columns`
}

type OutputFlag struct {
	People      bool
	Product     bool
	Campaign    bool
	Order       bool
	Consignment bool
	OrderDetail bool
	Event       bool
}

var ProjectID = os.Getenv("PROJECTID")

var PSPeople = os.Getenv("PSOUTPUTPEOPLE")
var PSEvent = os.Getenv("PSOUTPUTEVENT")
var PSProduct = os.Getenv("PSOUTPUTPRODUCT")
var PSCampaign = os.Getenv("PSOUTPUTCAMPAIGN")
var PSOrder = os.Getenv("PSOUTPUTORDER")
var PSConsignment = os.Getenv("PSOUTPUTCONSIGNMENT")
var PSOrderDetail = os.Getenv("PSOUTPUTORDERDETAIL")

var MLUrl = os.Getenv("PREDICTION")

var AIDataBucket = os.Getenv("AIBUCKET")
var RedisAddress = os.Getenv("MEMSTORE")

// global vars
var ctx context.Context
var ps *pubsub.Client
var topicPeople *pubsub.Topic
var topicEvent *pubsub.Topic
var topicProduct *pubsub.Topic
var topicCampaign *pubsub.Topic
var topicOrder *pubsub.Topic
var topicConsignment *pubsub.Topic
var topicOrderDetail *pubsub.Topic
var ai *ml.Service
var cs *storage.Client
var msPool *redis.Pool

var listCities map[string]bool
var listStates map[string]bool
var listCountries map[string]bool
var listFirstNames map[string]bool
var listLastNames map[string]bool
var listError error
var listCityStateZip []CityStateZip
var listChannels map[string]bool

var reEmail = regexp.MustCompile("(?i)^[a-zA-Z0-9.!#$%&'*+/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$")
var rePhone = regexp.MustCompile(`(?i)^(?:(?:\(?(?:00|\+)([1-4]\d\d|[1-9]\d?)\)?)?[\-\.\ \\\/]?)?((?:\(?\d{1,}\)?[\-\.\ \\\/]?){0,})(?:[\-\.\ \\\/]?(?:#|ext\.?|extension|x)[\-\.\ \\\/]?(\d+))?$`)
var reZipcode = regexp.MustCompile(`(?i)^\d{5}(?:[-\s]\d{4})?$`)
var reStreet1 = regexp.MustCompile(`(?i)\d{1,4} [\w\s]{1,20}(?:street|st|avenue|ave|road|rd|highway|hwy|square|sq|trail|trl|drive|dr|court|ct|park|parkway|pkwy|circle|cir|boulevard|blvd)\W?`)
var reStreet2 = regexp.MustCompile(`(?i)apartment|apt|unit|box`)
var reStreet3 = regexp.MustCompile(`(?i)apartment|apt|unit|box`)
var reNewline = regexp.MustCompile(`\r?\n`)

// MRT's version doesnt compile, substituting with a package
// var reBrowser = regexp.MustCompile(`(MSIE|Trident|(?!Gecko.+)Firefox|(?!AppleWebKit.+Chrome.+)Safari(?!.+Edge)|(?!AppleWebKit.+)Chrome(?!.+Edge)|(?!AppleWebKit.+Chrome.+Safari.+)Edge|AppleWebKit(?!.+Chrome|.+Safari)|Gecko(?!.+Firefox))(?: |\/)([\d\.apre]+)`)

var reCleanupDigitsOnly = regexp.MustCompile("[^a-zA-Z0-9]+")

func init() {
	ctx = context.Background()
	ps, _ = pubsub.NewClient(ctx, ProjectID)
	topicPeople = ps.Topic(PSPeople)
	topicEvent = ps.Topic(PSEvent)
	topicProduct = ps.Topic(PSProduct)
	topicCampaign = ps.Topic(PSCampaign)
	topicOrder = ps.Topic(PSOrder)
	topicConsignment = ps.Topic(PSConsignment)
	topicOrderDetail = ps.Topic(PSOrderDetail)

	ai, _ := ml.NewService(ctx)
	cs, _ = storage.NewClient(ctx)

	msPool = NewPool(RedisAddress)

	// preload the lists
	var err error

	listChannels = map[string]bool{"tablet": true, "mobile": true, "desktop": true}
	listCities, err = ReadJsonArray(ctx, cs, AIDataBucket, "data/cities.json")
	if err != nil {
		log.Fatalf("Failed to read json %v from bucket", "data/cities.json")
	} else {
		log.Printf("read %v values from %v", len(listCities), "data/cities.json")
	}

	listStates, err = ReadJsonArray(ctx, cs, AIDataBucket, "data/states.json")
	if err != nil {
		log.Fatalf("Failed to read json %v from bucket", "data/states.json")
	} else {
		log.Printf("read %v values from %v", len(listStates), "data/states.json")
	}

	listCountries, err = ReadJsonArray(ctx, cs, AIDataBucket, "data/countries.json")
	if err != nil {
		log.Fatalf("Failed to read json %v from bucket", "data/countries.json")
	} else {
		log.Printf("read %v values from %v", len(listCountries), "data/countries.json")
	}

	listFirstNames, err = ReadJsonArray(ctx, cs, AIDataBucket, "data/first_names.json")
	if err != nil {
		log.Fatalf("Failed to read json %v from bucket", "data/first_names.json")
	} else {
		log.Printf("read %v values from %v", len(listFirstNames), "data/first_names.json")
	}

	listLastNames, err = ReadJsonArray(ctx, cs, AIDataBucket, "data/last_names.json")
	if err != nil {
		log.Fatalf("Failed to read json %v from bucket", "data/last_names.json")
	} else {
		log.Printf("read %v values from %v", len(listLastNames), "data/last_names.json")
	}

	listCityStateZip, err = ReadCityStateZip(ctx, cs, AIDataBucket, "data/zip_city_state.json")
	if err != nil {
		log.Fatalf("Failed to read json %v from bucket", "data/zip_city_state.json")
	} else {
		log.Printf("read %v values from %v", len(listCityStateZip), "data/zip_city_state.json")
	}

	log.Printf("init completed, ai basepath %v, pubsub topic names: %v, %v, %v, %v, %v, %v, %v", ai.BasePath, topicPeople, topicEvent, topicProduct, topicCampaign, topicOrder, topicConsignment, topicOrderDetail)
}

func PreProcess(ctx context.Context, m PubSubMessage) error {
	var input Input
	if err := json.Unmarshal(m.Data, &input); err != nil {
		log.Fatalf("Unable to unmarshal message %v with error %v", string(m.Data), err)
	}

	columns := GetColumnsFromInput(input)

	// append attributes
	for k, v := range input.Attributes {
		attribute := InputColumn{
			Name:  k,
			Value: v,
		}
		columns = append(columns, attribute)
	}

	var flags OutputFlag
	var columnFlags ERRFlags

	// cycle through ALL columns running all ERRs
	for i, column := range columns {
		column.CampaignERR = GetCampaignERR(column.Name)
		column.ConsignmentERR = GetConsignmentERR(column.Name)
		column.EventERR = GetEventERR(column.Name)
		column.OrderERR = GetOrderERR(column.Name)
		column.OrderDetailERR = GetOrderDetailERR(column.Name)
		column.PeopleERR = GetPeopleERR(column.Name)
		column.ProductERR = GetProductERR(column.Name)

		log.Printf("column %v People ERR %v", column.Name, column.PeopleERR)

		if column.PeopleERR.FirstName == 1 {
			columnFlags.PeopleFirstName = true
		}
		if column.PeopleERR.LastName == 1 {
			columnFlags.PeopleLastName = true
		}
		if column.PeopleERR.Address1 == 1 {
			columnFlags.PeopleAddress1 = true
		}
		if column.PeopleERR.Address == 1 {
			columnFlags.PeopleAddress = true
		}
		if column.PeopleERR.ZipCode == 1 {
			columnFlags.PeopleZip = true
		}
		if column.PeopleERR.City == 1 {
			columnFlags.PeopleCity = true
		}
		if column.PeopleERR.Email == 1 {
			columnFlags.PeopleEmail = true
		}
		if column.PeopleERR.Phone == 1 {
			columnFlags.PeoplePhone = true
		}
		if column.PeopleERR.TrustedID == 1 {
			columnFlags.PeopleClientID = true
		}

		if column.ProductERR.PID == 1 {
			columnFlags.ProductID = true
		}

		if column.CampaignERR.CampaignID == 1 {
			columnFlags.CampaignID = true
		}

		if column.OrderERR.ID == 1 {
			columnFlags.OrderID = true
		}

		// do not flag this
		// if column.ConsignmentERR.ID == 1 {
		// 	columnFlags.ConsignmentID = true
		// }

		if column.OrderDetailERR.ID == 1 {
			columnFlags.OrderDetailID = true
		}
		if column.OrderDetailERR.ProductSKU == 1 {
			columnFlags.ProductSKU = true
		}
		if column.OrderDetailERR.ProductID == 1 {
			columnFlags.ProductID = true
		}
		if column.OrderDetailERR.OrderID == 1 {
			columnFlags.OrderID = true
		}

		column.Value = reNewline.ReplaceAllString(column.Value, " ")
		columns[i] = column
	}

	// update entity flags
	flags.Event = true // every record = event

	if columnFlags.PeopleFirstName && columnFlags.PeopleLastName && (columnFlags.PeopleZip || columnFlags.PeopleAddress) {
		flags.People = true
	}
	if columnFlags.PeopleLastName && columnFlags.PeopleAddress1 && columnFlags.PeopleCity {
		flags.People = true
		log.Printf("have a people entity >>> LastName, Add1, City")
	}
	if columnFlags.PeopleFirstName && columnFlags.PeoplePhone {
		flags.People = true
	}
	if columnFlags.PeopleEmail {
		flags.People = true
	}
	if columnFlags.PeopleClientID {
		flags.People = true
	}
	// if we don't have ANY columns... throw it to people to try out ver...
	if (!columnFlags.OrderID && !columnFlags.CampaignID && !columnFlags.ProductID && !columnFlags.PeopleClientID && !columnFlags.PeopleEmail && !columnFlags.PeopleFirstName && !columnFlags.PeoplePhone && !columnFlags.PeopleLastName && !columnFlags.PeopleZip ) {
		flags.People = true
	}

	if columnFlags.ProductID && columnFlags.ProductName {
		flags.Product = true
	}
	if columnFlags.CampaignID {
		flags.Campaign = true
	}
	if columnFlags.OrderID {
		flags.Order = true
	}
	// skip consignments
	// if columnFlags.ConsignmentID && columnFlags.OrderID {
	// 	flags.Consignment = true
	// }
	if (columnFlags.OrderDetailID && columnFlags.OrderID) || ((columnFlags.ProductID || columnFlags.ProductSKU) && columnFlags.OrderID) {
		flags.OrderDetail = true
	}
	log.Printf("columns %v", columns)
	log.Printf("column flags %v", columnFlags)
	log.Printf("entity flags %v", flags)

	// run VER
	for i, column := range columns {
		column.EventVER = GetEventVER(&column)
		if flags.People {
			column.PeopleVER = GetPeopleVER(&column)
		}
		columns[i] = column
	}

	// look up NER and call ML if People
	var prediction Prediction

	if flags.People {
		PeopleNERKey := GetNERKey(input.Signature, GetMapKeys(input.Fields))
		PeopleNER := FindNER(PeopleNERKey)

		if len(PeopleNER.Columns) > 0 {
			// copy NER into the columns
			for i, column := range columns {
				for _, ner := range PeopleNER.Columns {
					if strings.EqualFold(column.Name, ner.ColumnName) {
						MapNER(column, ner.NEREntities)
					}
				}
				columns[i] = column
			}
			log.Printf("columns %v", columns)
		}

		mlInput := BuildMLData(columns)
		log.Printf("columns: %v", columns)
		log.Printf("mlinput: %v", mlInput)
		mlJSON, _ := json.Marshal(mlInput)
		log.Printf("ML request %v", string(mlJSON))
		reqBody := &ml.GoogleApi__HttpBody{
			Data: string(mlJSON),
		}
		req := ml.GoogleCloudMlV1__PredictRequest{
			HttpBody: reqBody,
		}
		req.HttpBody.ContentType = "application/json"
		ai, _ = ml.NewService(ctx)
		mlPredict := ai.Projects.Predict(MLUrl, &req)
		r, err := mlPredict.Context(ctx).Do()
		if err != nil {
			log.Fatalf("error calling mlService, %v", err)
			return nil
		}

		if err := json.NewDecoder(strings.NewReader(r.Data)).Decode(&prediction); err != nil {
			if _, ok := err.(*json.SyntaxError); ok {
				log.Fatalf("error decoding json, %v", string(r.Data))
			}
		}
		if len(prediction.Predictions) == 0 {
			log.Fatalf("unexpected prediction returned, %v", string(r.Data))
		}
		log.Printf("ML result is %v", string(r.Data))
	}

	log.Printf("columns %v", columns)

	// pub
	var output Output
	output.Signature = input.Signature
	output.Passthrough = input.Passthrough
	output.Columns = columns
	output.Prediction = prediction

	outputJSON, _ := json.Marshal(output)
	if flags.People {
		PubMessage(topicPeople, outputJSON)
	}
	if flags.Product {
		PubMessage(topicProduct, outputJSON)
	}
	if flags.Event {
		PubMessage(topicEvent, outputJSON)
	}
	if flags.Campaign {
		PubMessage(topicCampaign, outputJSON)
	}
	if flags.Order {
		PubMessage(topicOrder, outputJSON)
	}
	if flags.Consignment {
		PubMessage(topicConsignment, outputJSON)
	}
	if flags.OrderDetail {
		PubMessage(topicOrderDetail, outputJSON)
	}

	return nil
}

func MapNER(column InputColumn, ner map[string]float64) {
	for k, v := range ner {
		switch k {
		case "FAC":
			column.NER.FAC = v
			break
		case "GPE":
			column.NER.GPE = v
			break
		case "LOC":
			column.NER.LOC = v
			break
		case "NORP":
			column.NER.NORP = v
			break
		case "ORG":
			column.NER.ORG = v
			break
		case "PERSON":
			column.NER.PERSON = v
			break
		case "PRODUCT":
			column.NER.PRODUCT = v
			break
		case "EVENT":
			column.NER.EVENT = v
			break
		case "WORKOFART":
			column.NER.WORKOFART = v
			break
		case "LAW":
			column.NER.LAW = v
			break
		case "LANGUAGE":
			column.NER.LANGUAGE = v
			break
		case "DATE":
			column.NER.DATE = v
			break
		case "TIME":
			column.NER.TIME = v
			break
		case "PERCENT":
			column.NER.PERCENT = v
			break
		case "MONEY":
			column.NER.MONEY = v
			break
		case "QUANTITY":
			column.NER.QUANTITY = v
			break
		case "ORDINAL":
			column.NER.ORDINAL = v
			break
		case "CARDINAL":
			column.NER.CARDINAL = v
			break

		}
	}
}

// this runs per column passed to it...
func GetPeopleERR(column string) PeopleERR {
	var err PeopleERR

	key := strings.ToLower(column)
	switch key {
		case "fname", "f name", "first name", "name first", "first_name", "first":
			err.FirstName = 1
		case "lname", "lname ", "l name ", "l name", "last name", "name last", "last":
			err.LastName = 1
		case "mi", "mi ", "mname", "m", "middle name":
			err.MiddleName = 1
		case "suffix", "jr., iii, etc.":
			err.Suffix = 1
		case "ad", "ad1", "ad1 ", "add1", "add 1", "address 1", "ad 1", "address line 1", "street line 1", "street address 1", "address1", "street", "street_line1", "street address line 1", "addr_line_1":
			err.Address1 = 1
		case "ad2", "add2", "ad 2", "address 2", "address line 2", "street line 2", "street address 2", "address2", "street_line2", "street 2", "street address line 2", "addr_line_2":
			err.Address2 = 1
		case "ad3", "add3", "ad 3", "address 3", "address line 3", "street line 3", "street address 3", "address3", "street_line3", "street 3", "street address line 3", "addr_line_3":
			err.Address3 = 1	
		case "city", "city ", "street city":
			err.City = 1
		case "state", "st", "state ", "state_province", "st ", "state province", "street state":
			err.State = 1
		case "zip", "zip code", "zip ", "postal_code", "postal code", "zip postcode", "street zip":
			err.ZipCode = 1
		case "citystzip", "city/st/zip ":
			err.City = 1
			err.State = 1
			err.ZipCode = 1
		case "county":
			err.County = 1
		case "country", "country (blank for us)":
			err.Country = 1
		case "email", "student email", "email ", "email1", "email address", "stu_email", "student e mail", "studentemail", "student personal email address", "student emails", "student e-mail", "student personal email", "student email address", "email2", "email_address_2", "student school email":
			err.Email = 1
		case "par_email", "par_email1", "parent e-mail", "par email", "parent email", "parent email address", "par_email2":
			err.Email = 1
			err.ParentEmail = 1
		case "gender", "m/f":
			err.Gender = 1
		case "pfname", "pfname1", "pfname2":
			err.ParentFirstName = 1
		case "plname", "plname1", "plname2":
			err.ParentLastName = 1
		case "phone", "phone1", "hphone", "cphone", "mphone":
			err.Phone = 1
		case "bday", "birthday":
			err.Birthday = 1
		case "age":
			err.Age = 1
		case "pname", "pname1", "pname2", "pname 1", "pname 2":
			err.ParentFirstName = 1
			err.ParentLastName = 1
			err.ParentName = 1
		case "fullname", "full name", "student name", "students name":
			err.FullName = 1
			err.FirstName = 1
			err.LastName = 1
		case "dorm", "hall", "building", "building name", "dormitory", "apartment", "fraternity", "residence":
			err.Dorm = 1
		case "room", "room number", "room #":
			err.Room = 1
		case "organization":
			err.Organization = 1
		case "title", "course year":
			err.Title = 1
		case "studentid", "student id":
			err.TrustedID = 1
	}

	if strings.Contains(key, "first") || strings.Contains(key, "fname") {
		err.FirstName = 1
	}
	if strings.Contains(key, "last") || strings.Contains(key, "lname") {
		err.LastName = 1
	}
	if strings.Contains(key, "country") {
		err.Country = 1
	}
	if strings.Contains(key, "email") {
		err.Email = 1
	}
	if strings.Contains(key, "address") || strings.Contains(key, "addr") {
		err.Address = 1
	}
	if strings.Contains(key, "city") {
		err.City = 1
	}
	if strings.Contains(key, "state") {
		err.State = 1
	}
	if strings.Contains(key, "zip") || strings.Contains(key, "postalcode") || strings.Contains(key, "postal code") {
		err.ZipCode = 1
	}
	if strings.Contains(key, "phone") {
		err.Phone = 1
	}

	// these should be looked up on a per owner basis
	if strings.Contains(key, "class") || strings.Contains(key, "year") || strings.Contains(key, "class year") {
		err.Title = 1
	}
	if strings.Contains(key, "parent") || strings.Contains(key, "emergency") || strings.Contains(key, "contact") || strings.Contains(key, "father") || strings.Contains(key, "mother") || strings.Contains(key, "purchaser") || strings.Contains(key, "gaurdian") {
		err.Role = 1
	}

	// correct some assignments
	if err.City == 1 || err.State == 1 || err.ZipCode == 1 || err.Email == 1 || err.Country == 1 {
		err.Address1 = 0
	}
	if strings.Contains(key, "first") && strings.Contains(key, "name") {
		err.Address1 = 0
	}
	if strings.Contains(key, "last") && strings.Contains(key, "name") {
		err.Address1 = 0
	}
	if err.Organization == 1 {
		err.FirstName = 0
		err.LastName = 0
	}
	if err.MiddleName == 1  {
		err.FirstName = 0
		err.LastName = 0
	}

	// get some types
	if err.Address1 == 1 || err.City == 1 || err.State == 1 || err.ZipCode == 1 || err.Email == 1 {
		// default to home address
		err.AddressTypeHome = 1
		if strings.Contains(key, "consignment") {
			err.AddressTypeShipping = 1
		} else if strings.Contains(key, "order") {
			err.AddressTypeBilling = 1
		} else if strings.Contains(key, "emergency") || strings.Contains(key, "permanent") || strings.Contains(key, "home") {
			err.AddressTypeHome = 1
		} else if err.Dorm == 1 {
			err.AddressTypeCampus = 1
		}
	}

	return err
}

func GetCampaignERR(column string) CampaignERR {
	var err CampaignERR
	key := strings.ToLower(column)
	switch key {
	case "campaign id", "campaignid", "campaign.id":
		err.CampaignID = 1
	case "campaign", "campaign name", "campaignname", "campaign.name":
		err.Name = 1
	case "campaign type", "campaigntype", "campaign.type":
		err.Type = 1
	case "campaign budget", "campaignbudget", "budget", "campaign.budget":
		err.Budget = 1
	case "campaign channel", "campaignchannel", "campaign.channel":
		err.Channel = 1
	case "campaign start date", "campaign startdate", "campaignstartdate", "campaign.startdate":
		err.StartDate = 1
	case "campaign end date", "campaign enddate", "campaignenddate", "campaignend.date":
		err.EndDate = 1
	}
	return err
}

func GetConsignmentERR(column string) ConsignmentERR {
	var err ConsignmentERR
	key := strings.ToLower(column)
	switch key {
	case "ship date", "shipdate":
		err.ShipDate = 1
	case "shipment", "consignment", "consignment id", "consignmentid":
		err.ID = 1
	}

	// adding logic for flattened order source
	if strings.Contains(key, "order.consignments") && strings.Contains(key, "consignments") && strings.Contains(key, ".id") {
		err.ID = 1
	}

	return err
}

func GetEventERR(column string) EventERR {
	var err EventERR
	key := strings.ToLower(column)
	switch key {
	case "event id", "eventid", "event.id":
		err.ID = 1
	case "event type", "eventtype", "event.type":
		err.Type = 1
	case "campaign id", "campaignid", "campaign.id":
		err.CampaignID = 1
	case "browser":
		err.Browser = 1
	case "channel":
		err.Channel = 1
	case "os":
		err.OS = 1
	case "domain":
		err.Domain = 1
	case "url":
		err.URL = 1
	case "geo", "lat", "long":
		err.Location = 1
	case "referrer":
		err.Referrer = 1
	case "searchterm":
		err.SearchTerm = 1
	}
	return err
}

func GetOrderERR(column string) OrderERR {
	var err OrderERR
	key := strings.ToLower(column)
	switch key {
	case "orderid", "order id", "invoiceid", "invoice id", "order.id":
		err.ID = 1
	case "order number", "ordernumber", "full order number", "full ordernumber",
		"fullorder number", "fullordernumber", "ecometryordernumber":
		err.Number = 1
	case "order date", "orderdate", "invoice date", "invoicedate",
		"placed date", "placeddate", "created at", "createdat":
		err.Date = 1
	case "order subtotal", "ordersubtotal", "subtotal":
		err.SubTotal = 1
	case "order discount", "orderdiscount", "discount":
		err.Discount = 1
	case "order shipping", "ordershipping", "shipping":
		err.Shipping = 1
	case "order tax", "ordertax", "tax":
		err.Tax = 1
	case "order total", "ordertotal", "total":
		err.Total = 1
	// for de-nested node case...	
	case "order.ecometryordernumber":
		err.Number = 1
	case "order.ektronuserid":
		err.CustomerID = 1
	case "order.placedat":
		err.Date = 1
	case "order.ordersubtotal", "order.subtotal":
		err.SubTotal = 1
	case "order.orderdiscount", "order.discount":
		err.Discount = 1
	case "order.ordershipping", "order.shipping":
		err.Shipping = 1
	case "order.ordertax", "order.tax":
		err.Tax = 1
	case "order.total":
		err.Total = 1
	}

	return err
}

func GetOrderDetailERR(column string) OrderDetailERR {
	var err OrderDetailERR
	key := strings.ToLower(column)
	switch key {
	case "order detail id", "orderdetail id", "orderdetailid", "row", "line":
		err.ID = 1
	}

	// adding logic for flattened order source
	if strings.Contains(key, "order.consignments") && strings.Contains(key, "shipments") && strings.Contains(key, "shipitems") && strings.Contains(key, ".id") {
		err.ID = 1
	}
	if strings.Contains(key, "ordernumber") {
		err.OrderNumber = 1
	}
	if strings.Contains(key, "order.consignments") && strings.Contains(key, "shipments") && strings.Contains(key, "shipitems") && strings.Contains(key, ".orderid") {
		err.OrderID = 1
	}
	if strings.Contains(key, "order.consignments") && strings.Contains(key, "shipments") && strings.Contains(key, "shipitems") && strings.Contains(key, ".productid") {
		err.ProductID = 1
	}
	if strings.Contains(key, "order.consignments") && strings.Contains(key, "shipments") && strings.Contains(key, "shipitems") && strings.Contains(key, ".itemsku") {
		err.ProductSKU = 1
	}
	if strings.Contains(key, "order.consignments") && strings.Contains(key, "shipments") && strings.Contains(key, "shipitems") && strings.Contains(key, ".quantity") {
		err.ProductQuantity = 1
	}

	return err
}

func GetProductERR(column string) ProductERR {
	var err ProductERR
	key := strings.ToLower(column)
	switch key {
	case "product name", "productname", "prod name", "prodname", "product.name":
		err.Name = 1
	case "product description", "productdescription", "prod description", "product.description",
		"proddescription", "product desc", "productdesc", "prod desc",
		"proddesc", "p desc", "pdesc":
		err.Description = 1
	case "product size", "productsize", "prod size", "product.size",
		"p size", "psize", "size":
		err.Size = 1
	case "product color", "productcolor", "prod color", "product.color",
		"p color", "pcolor", "color":
		err.Color = 1
	case "product unit price", "productunit price", "prod unit price", "product.unitprice",
		"product unitprice", "productunitprice", "prod unitprice",
		"p unit price", "punit price", "p unitprice", "punitprice",
		"unit price", "unitprice":
		err.UnitPrice = 1
	case "product type", "producttype", "prod type", "product.type",
		"p type", "ptype", "type":
		err.Type = 1
	case "product vendorid", "productvendorid", "prod vendorid",
		"p vendorid", "pvendorid", "vendorid":
		err.VendorID = 1
	case "product vendor", "productvendor", "prod vendor",
		"p vendor", "pvendor", "vendor":
		err.Vendor = 1
	case "product cost", "productcost", "prod cost", "product.cost",
		"p cost", "pcost", "cost":
		err.Cost = 1
	case "product stars", "productstars", "prod stars", "product.stars",
		"p stars", "pstars", "stars":
		err.Stars = 1
	case "product category", "productcategory", "product cat", "product.category",
		"productcat", "prod cat", "prodcat", "p cat", "pcat":
		err.Category = 1
	case "product margin", "productmargin", "prod margin", "product.margin",
		"p margin", "pmargin", "margin", "contibution":
		err.Margin = 1
	case "contains", "bundle items", "bundleitems", "bundled items", "bundleditems",
		"kit items", "kititems":
		err.Contains = 1
	}
	return err
}

func PubMessage(topic *pubsub.Topic, data []byte) {
	psresult := topic.Publish(ctx, &pubsub.Message{
		Data: data,
	})
	psid, err := psresult.Get(ctx)
	_, err = psresult.Get(ctx)
	if err != nil {
		log.Fatalf("Could not pub to pubsub: %v", err)
	} else {
		log.Printf("pubbed to %v as message id %v: %v", topic, psid, string(data))
	}
}

func GetColumnsFromInput(input Input) []InputColumn {
	var columns []InputColumn

	for k, v := range input.Fields {
		column := InputColumn{
			Name:      k,
			Value:     v,
			PeopleERR: PeopleERR{},
			NER:       NER{},
			PeopleVER: PeopleVER{},
		}
		columns = append(columns, column)
	}
	return columns
}

func GetMapKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func GetLowerCaseSorted(m []string) []string {
	var result []string
	for _, k := range m {
		result = append(result, strings.ToLower(k))
	}
	sort.Strings(result)
	return result
}

func GetNERKey(sig Signature, columns []string) string {
	// concatenate all columnn headers together, in lower case
	keys := strings.Join(GetLowerCaseSorted(columns[:]), "-")
	hasher := sha1.New()
	io.WriteString(hasher, keys)
	return fmt.Sprintf("ner:%v:%v:%v:%x", sig.OwnerID, strings.ToLower(sig.Source), strings.ToLower(sig.EventType), hasher.Sum(nil))
}

func FindNER(key string) NERCache {
	var ner NERCache
	ms := msPool.Get()
	s, err := redis.String(ms.Do("GET", key))
	if err == nil {
		json.Unmarshal([]byte(s), &ner)
	}
	return ner
}

func GetPeopleVER(column *InputColumn) PeopleVER {
	var val = strings.TrimSpace(column.Value)
	log.Printf("features values is %v", val)
	val = RemoveDiacritics(val)
	result := PeopleVER{
		HASHCODE:     int64(GetHash(val)),
		IS_FIRSTNAME: ContainsBool(listFirstNames, val),
		IS_LASTNAME:  ContainsBool(listLastNames, val),
		IS_STREET1:   reStreet1.MatchString(val),
		IS_STREET2:   reStreet2.MatchString(val),
		IS_STREET3:   reStreet3.MatchString(val),
		IS_CITY:      ContainsBool(listCities, val),
		IS_STATE:     ContainsBool(listStates, val),
		IS_ZIPCODE:   reZipcode.MatchString(val),
		IS_COUNTRY:   ContainsBool(listCountries, val),
		IS_EMAIL:     reEmail.MatchString(val),
		IS_PHONE:     rePhone.MatchString(val) && len(val) >= 10,
	}
	columnJ, _ := json.Marshal(result)
	log.Printf("current VER %v", string(columnJ))
	return result
}

func GetEventVER(column *InputColumn) EventVER {
	var val = strings.TrimSpace(column.Value)
	log.Printf("features values is %v", val)
	val = RemoveDiacritics(val)
	browser := useragent.Parse(val)
	isBrowser := true
	if browser == nil {
		isBrowser = false
	}
	result := EventVER{
		IS_BROWSER: isBrowser,
		IS_CHANNEL: ContainsBool(listChannels, val),
	}
	columnJ, _ := json.Marshal(result)
	log.Printf("current VER %v", string(columnJ))
	return result
}

func GetHash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func BuildMLData(cols []InputColumn) MLInput {
	var instances [][]float64
	for _, column := range cols {
		var instance []float64
		instance = append(instance, float64(column.PeopleERR.FirstName))
		instance = append(instance, float64(column.PeopleERR.LastName))
		instance = append(instance, float64(column.PeopleERR.MiddleName))
		instance = append(instance, float64(column.PeopleERR.Suffix))
		instance = append(instance, float64(column.PeopleERR.FullName))
		instance = append(instance, float64(column.PeopleERR.Address1))
		instance = append(instance, float64(column.PeopleERR.Address2))
		instance = append(instance, float64(column.PeopleERR.City))
		instance = append(instance, float64(column.PeopleERR.State))
		instance = append(instance, float64(column.PeopleERR.ZipCode))
		instance = append(instance, float64(column.PeopleERR.County))
		instance = append(instance, float64(column.PeopleERR.Country))
		instance = append(instance, float64(column.PeopleERR.Email))
		instance = append(instance, float64(column.PeopleERR.ParentEmail))
		instance = append(instance, float64(column.PeopleERR.Gender))
		instance = append(instance, float64(column.PeopleERR.Phone))
		instance = append(instance, float64(column.PeopleERR.ParentFirstName))
		instance = append(instance, float64(column.PeopleERR.ParentLastName))
		instance = append(instance, float64(column.PeopleERR.Birthday))
		instance = append(instance, float64(column.PeopleERR.Age))
		instance = append(instance, float64(column.PeopleERR.ParentName))
		instance = append(instance, float64(column.NER.PERSON))
		instance = append(instance, float64(column.NER.NORP))
		instance = append(instance, float64(column.NER.FAC))
		instance = append(instance, float64(column.NER.ORG))
		instance = append(instance, float64(column.NER.GPE))
		instance = append(instance, float64(column.NER.LOC))
		instance = append(instance, float64(column.NER.PRODUCT))
		instance = append(instance, float64(column.NER.EVENT))
		instance = append(instance, float64(column.NER.WORKOFART))
		instance = append(instance, float64(column.NER.LAW))
		instance = append(instance, float64(column.NER.LANGUAGE))
		instance = append(instance, float64(column.NER.DATE))
		instance = append(instance, float64(column.NER.TIME))
		instance = append(instance, float64(column.NER.PERCENT))
		instance = append(instance, float64(column.NER.MONEY))
		instance = append(instance, float64(column.NER.QUANTITY))
		instance = append(instance, float64(column.NER.ORDINAL))
		instance = append(instance, float64(column.NER.CARDINAL))
		instance = append(instance, float64(column.PeopleVER.HASHCODE))
		instance = append(instance, float64(ToUInt32(column.PeopleVER.IS_FIRSTNAME)))
		instance = append(instance, float64(ToUInt32(column.PeopleVER.IS_LASTNAME)))
		instance = append(instance, float64(ToUInt32(column.PeopleVER.IS_STREET1)))
		instance = append(instance, float64(ToUInt32(column.PeopleVER.IS_STREET2)))
		instance = append(instance, float64(ToUInt32(column.PeopleVER.IS_CITY)))
		instance = append(instance, float64(ToUInt32(column.PeopleVER.IS_STATE)))
		instance = append(instance, float64(ToUInt32(column.PeopleVER.IS_ZIPCODE)))
		instance = append(instance, float64(ToUInt32(column.PeopleVER.IS_COUNTRY)))
		instance = append(instance, float64(ToUInt32(column.PeopleVER.IS_EMAIL)))
		instance = append(instance, float64(ToUInt32(column.PeopleVER.IS_PHONE)))

		instances = append(instances, instance)
	}
	var mlInput MLInput
	mlInput.Instances = instances
	return mlInput
}

func ReadJsonArray(ctx context.Context, client *storage.Client, bucket, object string) (map[string]bool, error) {
	rc, err := client.Bucket(bucket).Object(object).NewReader(ctx)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	data, err := ioutil.ReadAll(rc)
	if err != nil {
		return nil, err
	}
	var intermediate []string
	json.Unmarshal(data, &intermediate)

	result := make(map[string]bool)
	for _, s := range intermediate {
		result[s] = true
	}
	return result, nil
}

func ReadJsonMap(ctx context.Context, client *storage.Client, bucket, object string) (map[string]string, error) {
	rc, err := client.Bucket(bucket).Object(object).NewReader(ctx)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	data, err := ioutil.ReadAll(rc)
	if err != nil {
		return nil, err
	}
	var result map[string]string
	json.Unmarshal(data, &result)

	return result, nil
}

func ReadCityStateZip(ctx context.Context, client *storage.Client, bucket, object string) ([]CityStateZip, error) {
	var result []CityStateZip
	rc, err := client.Bucket(bucket).Object(object).NewReader(ctx)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	data, err := ioutil.ReadAll(rc)
	if err != nil {
		return nil, err
	}
	json.Unmarshal(data, &result)
	return result, nil
}

func RemoveDiacritics(value string) string {
	t := transform.Chain(norm.NFD, transform.RemoveFunc(IsMn), norm.NFC)
	result, _, _ := transform.String(t, value)
	return result
}

func Contains(dict map[string]bool, key string) uint32 {
	if _, ok := dict[strings.ToUpper(key)]; ok {
		return 1
	}
	return 0
}

func ContainsBool(dict map[string]bool, key string) bool {
	if _, ok := dict[strings.ToUpper(key)]; ok {
		return true
	}
	return false
}

func ToUInt32(val bool) uint32 {
	if val {
		return 1
	}
	return 0
}

func IsMn(r rune) bool {
	return unicode.Is(unicode.Mn, r) // Mn: nonspacing marks
}

func NewPool(addr string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial:        func() (redis.Conn, error) { return redis.Dial("tcp", addr) },
	}
}
