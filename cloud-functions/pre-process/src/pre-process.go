package preprocess

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"
	"time"

	"cloud.google.com/go/datastore"
	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"google.golang.org/api/ml/v1"

	"github.com/gomodule/redigo/redis"
)

var ProjectID = os.Getenv("PROJECTID")
var DSProjectID = os.Getenv("DSPROJECTID")

var PSPeople = os.Getenv("PSOUTPUTPEOPLE")
var PSEvent = os.Getenv("PSOUTPUTEVENT")
var PSProduct = os.Getenv("PSOUTPUTPRODUCT")
var PSCampaign = os.Getenv("PSOUTPUTCAMPAIGN")
var PSOrder = os.Getenv("PSOUTPUTORDER")
var PSConsignment = os.Getenv("PSOUTPUTCONSIGNMENT")
var PSOrderDetail = os.Getenv("PSOUTPUTORDERDETAIL")
var Env = os.Getenv("ENVIRONMENT")
var dev = Env == "dev"
var DSKind = os.Getenv("DSKIND")

var MLUrl = os.Getenv("PREDICTION")

var AIDataBucket = os.Getenv("AIBUCKET")

var redisTransientExpiration = 3600 * 24

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
var msp *redis.Pool
var ds *datastore.Client
var fs *datastore.Client
var topicR *pubsub.Topic

var cfName = os.Getenv("FUNCTION_NAME")

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

	topicR = ps.Topic(os.Getenv("PSREPORT"))

	ai, _ := ml.NewService(ctx)
	cs, _ = storage.NewClient(ctx)
	ds, _ = datastore.NewClient(ctx, ProjectID)
	fs, _ = datastore.NewClient(ctx, DSProjectID)

	msp = &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial:        func() (redis.Conn, error) { return redis.Dial("tcp", os.Getenv("MEMSTORE")) },
	}

	// preload the lists
	var err error

	listChannels = map[string]bool{"tablet": true, "mobile": true, "desktop": true}
	listCities, err = ReadJSONArray(ctx, cs, AIDataBucket, "data/cities.json")
	if err != nil {
		// log.Fatalf("Failed to read json %v from bucket", "data/cities.json")
	} else {
		log.Printf("read %v values from %v", len(listCities), "data/cities.json")
	}

	listStates, err = ReadJSONArray(ctx, cs, AIDataBucket, "data/states.json")
	if err != nil {
		// log.Fatalf("Failed to read json %v from bucket", "data/states.json")
	} else {
		log.Printf("read %v values from %v", len(listStates), "data/states.json")
	}

	listCountries, err = ReadJSONArray(ctx, cs, AIDataBucket, "data/countries.json")
	if err != nil {
		// log.Fatalf("Failed to read json %v from bucket", "data/countries.json")
	} else {
		log.Printf("read %v values from %v", len(listCountries), "data/countries.json")
	}

	listFirstNames, err = ReadJSONArray(ctx, cs, AIDataBucket, "data/first_names.json")
	if err != nil {
		// log.Fatalf("Failed to read json %v from bucket", "data/first_names.json")
	} else {
		log.Printf("read %v values from %v", len(listFirstNames), "data/first_names.json")
	}

	listLastNames, err = ReadJSONArray(ctx, cs, AIDataBucket, "data/last_names.json")
	if err != nil {
		// log.Fatalf("Failed to read json %v from bucket", "data/last_names.json")
	} else {
		log.Printf("read %v values from %v", len(listLastNames), "data/last_names.json")
	}

	listCityStateZip, err = ReadCityStateZip(ctx, cs, AIDataBucket, "data/zip_city_state.json")
	if err != nil {
		// log.Fatalf("Failed to read json %v from bucket", "data/zip_city_state.json")
	} else {
		log.Printf("read %v values from %v", len(listCityStateZip), "data/zip_city_state.json")
	}

	log.Printf("init completed, ai basepath %v, pubsub topic names: %v, %v, %v, %v, %v, %v, %v", ai.BasePath, topicPeople, topicEvent, topicProduct, topicCampaign, topicOrder, topicConsignment, topicOrderDetail)
}

func PreProcess(ctx context.Context, m PubSubMessage) error {
	var input Input
	if err := json.Unmarshal(m.Data, &input); err != nil {
		log.Fatalf("Error: Unable to unmarshal message %v with error %v", string(m.Data), err)
	}

	dsNamespace := strings.ToLower(fmt.Sprintf("%v-%v", Env, input.Signature.OwnerID))

	LogDev(fmt.Sprintf("received input with signature: %v", input.Signature))

	// see if the record already exists, discard if it is
	existingCheck := GetRedisIntValue([]string{input.Signature.EventID, input.Signature.RecordID, "record"})
	if existingCheck > 0 {
		LogDev(fmt.Sprintf("RecordID already exists, abandoning: %v", input.Signature))
		return nil
	}
	// var existing []datastore.Key
	// if _, err := fs.GetAll(ctx, datastore.NewQuery(DSKind).Namespace(dsNamespace).Filter("RecordID =", input.Signature.RecordID).KeysOnly(), &existing); err != nil {
	// 	log.Printf("Error querying existing records: %v", err)
	// }
	// if len(existing) > 0 {
	// 	LogDev(fmt.Sprintf("RecordID already exists, abandoning: %v", input.Signature))
	// 	return nil
	// }

	if len(cfName) == 0 {
		cfName = "pre-process"
	}

	if len(input.Fields) > 0 {
		for k, v := range input.Fields {
			input.Fields[k] = strings.TrimSpace(v)
			if strings.EqualFold(input.Fields[k], "NULL") {
				input.Fields[k] = ""
			}
		}
	} else {
		report := FileReport{
			ID: input.Signature.EventID,
			Counters: []ReportCounter{
				ReportCounter{
					Type:      "PreProcess",
					Name:      "Empty",
					Count:     1,
					Increment: true,
				},
			},
		}
		publishReport(&report, cfName)

		// empty field list
		IncrRedisValue([]string{input.Signature.EventID, "records-deleted"})
		return nil
	}

	columns := GetColumnsFromInput(input)

	// append attributes
	for k, v := range input.Attributes {
		attribute := InputColumn{
			Name:        k,
			Value:       v,
			IsAttribute: true,
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

		// log.Printf("column %v People ERR %v", column.Name, column.PeopleERR)
		// this is going to be tight around name address... for entity detection could relax this...
		if column.PeopleERR.FirstName == 1 || column.PeopleERR.ContainsFirstName == 1 {
			columnFlags.PeopleFirstName = true
		}
		if column.PeopleERR.LastName == 1 || column.PeopleERR.ContainsLastName == 1 {
			columnFlags.PeopleLastName = true
		}
		if column.PeopleERR.FullName == 1 {
			columnFlags.PeopleFullName = true
		}
		if column.PeopleERR.Address1 == 1 {
			columnFlags.PeopleAddress1 = true
		}
		if column.PeopleERR.ContainsAddress == 1 {
			columnFlags.PeopleAddress = true
		}
		if column.PeopleERR.ZipCode == 1 || column.PeopleERR.ContainsZipCode == 1 {
			columnFlags.PeopleZip = true
		}
		if column.PeopleERR.City == 1 || column.PeopleERR.ContainsCity == 1 {
			columnFlags.PeopleCity = true
		}
		if column.PeopleERR.ContainsEmail == 1 {
			columnFlags.PeopleEmail = true
		}
		if column.PeopleERR.ContainsPhone == 1 {
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

	if (columnFlags.PeopleFirstName && columnFlags.PeopleLastName) && (columnFlags.PeopleZip || columnFlags.PeopleAddress || columnFlags.PeopleAddress1) {
		report := FileReport{
			ID: input.Signature.EventID,
			Counters: []ReportCounter{
				ReportCounter{
					Type:      "PreProcess",
					Name:      "Person:(FName+LName)&&(Zip|Address|Address1)",
					Count:     1,
					Increment: true,
				},
			},
		}
		publishReport(&report, cfName)

		flags.People = true
	} else if columnFlags.PeopleFirstName && columnFlags.PeopleAddress1 && columnFlags.PeopleCity {
		report := FileReport{
			ID: input.Signature.EventID,
			Counters: []ReportCounter{
				ReportCounter{
					Type:      "PreProcess",
					Name:      "Person:FName+Address1+City",
					Count:     1,
					Increment: true,
				},
			},
		}
		publishReport(&report, cfName)
		flags.People = true
		if dev {
			log.Printf("have a people entity >>> FName, Add1, City %v", input.Signature.EventID)
		}
	} else if columnFlags.PeopleLastName && columnFlags.PeopleAddress1 && columnFlags.PeopleCity {
		report := FileReport{
			ID: input.Signature.EventID,
			Counters: []ReportCounter{
				ReportCounter{
					Type:      "PreProcess",
					Name:      "Person:LName+Address1+City",
					Count:     1,
					Increment: true,
				},
			},
		}
		publishReport(&report, cfName)
		flags.People = true
		if dev {
			log.Printf("have a people entity >>> LastName, Add1, City %v", input.Signature.EventID)
		}
	} else if columnFlags.PeopleLastName && columnFlags.PeopleAddress && columnFlags.PeopleZip {
		report := FileReport{
			ID: input.Signature.EventID,
			Counters: []ReportCounter{
				ReportCounter{
					Type:      "PreProcess",
					Name:      "Person:LName+Address+Zip",
					Count:     1,
					Increment: true,
				},
			},
		}
		publishReport(&report, cfName)
		flags.People = true
		if dev {
			log.Printf("have a people entity >>> LastName, Add, Zip %v", input.Signature.EventID)
		}
	} else if columnFlags.PeopleFirstName && columnFlags.PeoplePhone {
		report := FileReport{
			ID: input.Signature.EventID,
			Counters: []ReportCounter{
				ReportCounter{
					Type:      "PreProcess",
					Name:      "Person:FName+Phone",
					Count:     1,
					Increment: true,
				},
			},
		}
		publishReport(&report, cfName)
		flags.People = true
	} else if columnFlags.PeopleEmail {
		report := FileReport{
			ID: input.Signature.EventID,
			Counters: []ReportCounter{
				ReportCounter{
					Type:      "PreProcess",
					Name:      "Person:Email",
					Count:     1,
					Increment: true,
				},
			},
		}
		publishReport(&report, cfName)
		flags.People = true
		if dev {
			log.Printf("have a people entity >>> Email %v", input.Signature.EventID)
		}
	} else if columnFlags.PeopleClientID {
		report := FileReport{
			ID: input.Signature.EventID,
			Counters: []ReportCounter{
				ReportCounter{
					Type:      "PreProcess",
					Name:      "Person:ClientID",
					Count:     1,
					Increment: true,
				},
			},
		}
		publishReport(&report, cfName)
		flags.People = true
		if dev {
			log.Printf("have a people entity >>> ClientId %v", input.Signature.EventID)
		}
	}

	// // if we don't have ANY columns... throw it to people to try out ver...
	// if !columnFlags.OrderID && !columnFlags.CampaignID && !columnFlags.ProductID && !columnFlags.PeopleClientID && !columnFlags.PeopleEmail && !columnFlags.PeopleFirstName && !columnFlags.PeoplePhone && !columnFlags.PeopleLastName && !columnFlags.PeopleZip {
	// 	flags.People = true
	// 	if dev {
	// 		log.Printf("have a people entity >>> Headless %v", input.Signature.EventID)
	// 	}
	// }

	if columnFlags.ProductID && columnFlags.ProductName {
		flags.Product = true
	}
	if columnFlags.CampaignID {
		flags.Campaign = true
	}
	if columnFlags.OrderID {
		flags.Order = true
	}

	if (columnFlags.OrderDetailID && columnFlags.OrderID) || ((columnFlags.ProductID || columnFlags.ProductSKU) && columnFlags.OrderID) {
		flags.OrderDetail = true
	}

	if flags.OrderDetail { // unset order if order detail is set
		flags.Order = false
	}

	if dev {
		log.Printf("entity flags %v %v", flags, input.Signature.EventID)
	}
	if dev {
		log.Printf("column flags %v %v", columnFlags, input.Signature.EventID)
	}
	if dev {
		log.Printf("columns %v %v", columns, input.Signature.EventID)
	}

	// run VER
	for i, column := range columns {
		column.EventVER = GetEventVER(&column)
		if flags.People {
			column.PeopleVER = GetPeopleVER(&column)
		}
		columns[i] = column
	}

	existingCheck = GetRedisIntValue([]string{input.Signature.EventID, input.Signature.RecordID, "record"})

	if existingCheck == 0 {
		// store RECORD in DS
		immutableDS := RecordDS{
			EventID:       input.Signature.EventID,
			EventType:     input.Signature.EventType,
			RecordID:      input.Signature.RecordID,
			RowNumber:     input.Signature.RowNumber,
			Fields:        ToKVPSlice(&input.Fields),
			TimeStamp:     time.Now(),
			IsPeople:      flags.People,
			IsProduct:     flags.Product,
			IsCampaign:    flags.Campaign,
			IsOrder:       flags.Order,
			IsConsignment: flags.Consignment,
			IsOrderDetail: flags.OrderDetail,
			IsEvent:       flags.Event,
			MLError:       false,
		}

		dsKey := datastore.IncompleteKey(DSKind, nil)
		dsKey.Namespace = dsNamespace
		if _, err := fs.Put(ctx, dsKey, &immutableDS); err != nil {
			log.Fatalf("Exception storing record kind %v sig %v, error %v", DSKind, input.Signature, err)
		}

		// pub
		var output Output
		output.Signature = input.Signature
		output.Passthrough = input.Passthrough
		output.Columns = columns
		// output.Prediction = prediction

		outputJSON, _ := json.Marshal(output)
		if flags.People {
			report := FileReport{
				ID: input.Signature.EventID,
				Counters: []ReportCounter{
					ReportCounter{
						Type:      "PreProcess",
						Name:      "IsPerson",
						Count:     1,
						Increment: true,
					},
				},
			}
			publishReport(&report, cfName)

			SetRedisKeyWithExpiration([]string{input.Signature.EventID, input.Signature.RecordID, "record"})
			IncrRedisValue([]string{input.Signature.EventID, "records-completed"})
			PubMessage(topicPeople, outputJSON)
		} else {
			report := FileReport{
				ID: input.Signature.EventID,
				Counters: []ReportCounter{
					ReportCounter{
						Type:      "PreProcess",
						Name:      "IsNotPerson",
						Count:     1,
						Increment: true,
					},
				},
			}
			publishReport(&report, cfName)

			SetRedisKeyWithExpiration([]string{input.Signature.EventID, input.Signature.RecordID, "record"})
			IncrRedisValue([]string{input.Signature.EventID, "records-deleted"})
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
	} else {
		report := FileReport{
			ID: input.Signature.EventID,
			Counters: []ReportCounter{
				ReportCounter{
					Type:      "PreProcess",
					Name:      "Existing",
					Count:     1,
					Increment: true,
				},
			},
		}
		publishReport(&report, cfName)
	}

	return nil
}
