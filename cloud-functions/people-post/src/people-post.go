package peoplepost

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"

	"github.com/ulule/deepcopier"
)

// PubSubMessage is the payload of a pubsub event
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

type Prediction struct {
	Predictions []float64 `json:"predictions"`
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
	Name           string         `json:"Name"`
	Value          string         `json:"Value"`
	MatchKey       string         `json:"MK"`
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
	MatchKeys   PeopleOutput      `json:"matchkeys`
}

type MatchKeyField struct {
	Value  string `json:"value"`
	Source string `json:"source"`
	Type   string `json:"type"`
}

type PeopleOutput struct {
	FNAME   MatchKeyField `json:"fname"`
	LNAME   MatchKeyField `json:"lname"`
	CITY    MatchKeyField `json:"city"`
	STATE   MatchKeyField `json:"state"`
	ZIP     MatchKeyField `json:"zip"`
	COUNTRY MatchKeyField `json:"country"`
	EMAIL   MatchKeyField `json:"email"`
	PHONE   MatchKeyField `json:"phone"`
	AD1     MatchKeyField `json:"ad1"`
	AD2     MatchKeyField `json:"ad2"`
	ADTYPE  MatchKeyField `json:"adType"`

	TRUSTEDID MatchKeyField `json:"trustedId"`

	CLIENTID   MatchKeyField `json:"clientId"`
	SALUTATION MatchKeyField `json:"salutation"`
	NICKNAME   MatchKeyField `json:"nickname"`

	GENDER MatchKeyField `json:"gender"`
	AGE    MatchKeyField `json:"age"`
	DOB    MatchKeyField `json:"dob"`

	MAILROUTE MatchKeyField `json:"mailRoute"`

	ORGANIZATION MatchKeyField `json:"organization"`
	TITLE        MatchKeyField `json:"title"`
	ROLE         MatchKeyField `json:"role"`
	STATUS       MatchKeyField `json:"status"`
}

type PeopleERR struct {
	Address1            int `json:"Address1"`
	Address2            int `json:"Address2"`
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
	AddressTypeCampus   int `json:"ATCampus"`
	AddressTypeHome     int `json:"ATHome"`
	AddressTypeBilling  int `json:"ATBilling"`
	AddressTypeShipping int `json:"ATShipping"`
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

type OrderERR struct {
	ID         int `json:"ID"`
	Number     int `json:"Number"`
	CustomerID int `json:"CustomerID"`
	Date       int `json:"Date"`
	Total      int `json:"Total"`
	BillTo     int `json:"BillTo"`
}

type ConsignmentERR struct {
	ID       int `json:"ID"`
	ShipDate int `json:"ShipDate"`
}

type OrderDetailERR struct {
	ID           int `json:"ID"`
	OrderID      int `json:"OrderID"`
	ConsigmentID int `json:"ConsigmentID"`
	ProductID    int `json:"ProductID"`
	ProductSKU   int `json:"ProductSKU"`
	ProductUPC   int `json:"ProductUPC"`
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
	IS_CITY      bool  `json:"isCITY"`
	IS_STATE     bool  `json:"isSTATE"`
	IS_ZIPCODE   bool  `json:"isZIPCODE"`
	IS_COUNTRY   bool  `json:"isCOUNTRY"`
	IS_EMAIL     bool  `json:"isEMAIL"`
	IS_PHONE     bool  `json:"isPHONE"`
}

type SmartyStreetResponse []struct {
	InputIndex           int    `json:"input_index"`
	CandidateIndex       int    `json:"candidate_index"`
	DeliveryLine1        string `json:"delivery_line_1"`
	LastLine             string `json:"last_line"`
	DeliveryPointBarcode string `json:"delivery_point_barcode"`
	Components           struct {
		PrimaryNumber           string `json:"primary_number"`
		StreetPredirection      string `json:"street_predirection"`
		StreetName              string `json:"street_name"`
		StreetSuffix            string `json:"street_suffix"`
		SecondaryNumber         string `json:"secondary_number"`
		SecondaryDesignator     string `json:"secondary_designator"`
		CityName                string `json:"city_name"`
		DefaultCityName         string `json:"default_city_name"`
		StateAbbreviation       string `json:"state_abbreviation"`
		Zipcode                 string `json:"zipcode"`
		Plus4Code               string `json:"plus4_code"`
		DeliveryPoint           string `json:"delivery_point"`
		DeliveryPointCheckDigit string `json:"delivery_point_check_digit"`
	} `json:"components"`
	Metadata struct {
		RecordType            string  `json:"record_type"`
		ZipType               string  `json:"zip_type"`
		CountyFips            string  `json:"county_fips"`
		CountyName            string  `json:"county_name"`
		CarrierRoute          string  `json:"carrier_route"`
		CongressionalDistrict string  `json:"congressional_district"`
		Rdi                   string  `json:"rdi"`
		ElotSequence          string  `json:"elot_sequence"`
		ElotSort              string  `json:"elot_sort"`
		Latitude              float64 `json:"latitude"`
		Longitude             float64 `json:"longitude"`
		Precision             string  `json:"precision"`
		TimeZone              string  `json:"time_zone"`
		UtcOffset             int     `json:"utc_offset"`
		Dst                   bool    `json:"dst"`
	} `json:"metadata"`
	Analysis struct {
		DpvMatchCode string `json:"dpv_match_code"`
		DpvFootnotes string `json:"dpv_footnotes"`
		DpvCmra      string `json:"dpv_cmra"`
		DpvVacant    string `json:"dpv_vacant"`
		Active       string `json:"active"`
		Footnotes    string `json:"footnotes"`
	} `json:"analysis"`
}

type AddressParsed struct {
	Number      string `json:"number"`
	Street      string `json:"street"`
	Type        string `json:"type"`
	SecUnitType string `json:"sec_unit_type"`
	SecUnitNum  string `json:"sec_unit_num"`
	City        string `json:"city"`
	State       string `json:"state"`
	Zip         string `json:"zip"`
	Plus4       string `json:"plus4"`
}

type MultiPersonRecord struct {
	FNAME       string
	FNAMEColumn string
	LNAME       string
	LNAMEColumn string
	EMAIL       string
	EMAILColumn string
}

type CityStateZip struct {
	Cities []string `json:"cities"`
	State  string   `json:"state"`
	Zip    string   `json:"zip"`
}

var ProjectID = os.Getenv("PROJECTID")
var PubSubTopic = os.Getenv("PSOUTPUT")
var PubSubTopic2 = os.Getenv("PSOUTPUT2")

var SmartyStreetsEndpoint = os.Getenv("SMARTYSTREET")
var AddressParserEndpoint = os.Getenv("ADDRESSPARSE")

var StorageBucket = os.Getenv("CLOUDSTORAGE")

var reGraduationYear = regexp.MustCompile(`20^\d{2}$`)
var reNumberOnly = regexp.MustCompile("[^0-9]+")
var reConcatenatedAddress = regexp.MustCompile(`(\d*)\s+((?:[\w+\s*\-])+)[\,]\s+([a-zA-Z]+)\s+([0-9a-zA-Z]+)`)
var reConcatenatedCityStateZip = regexp.MustCompile(`((?:[\w+\s*\-])+)[\,]\s+([a-zA-Z]+)\s+([0-9a-zA-Z]+)`)
var reNewline = regexp.MustCompile(`\r?\n`)
var reResidenceHall = regexp.MustCompile(`(?i)\sALPHA|ALUMNI|APARTMENT|APTS|BETA|BUILDING|CAMPUS|CENTENNIAL|CENTER|CHI|COLLEGE|COMMON|COMMUNITY|COMPLEX|COURT|CROSS|DELTA|DORM|EPSILON|ETA|FOUNDER|FOUNTAIN|FRATERNITY|GAMMA|GARDEN|GREEK|HALL|HEIGHT|HERITAGE|HIGH|HILL|HOME|HONOR|HOUS|INN|INTERNATIONAL|IOTA|KAPPA|LAMBDA|LANDING|LEARNING|LIVING|LODGE|MEMORIAL|MU|NU|OMEGA|OMICRON|PARK|PHASE|PHI|PI|PLACE|PLAZA|PSI|RESIDEN|RHO|RIVER|SCHOLARSHIP|SIGMA|SQUARE|STATE|STUDENT|SUITE|TAU|TERRACE|THETA|TOWER|TRADITIONAL|UNIV|UNIVERSITY|UPSILON|VIEW|VILLAGE|VISTA|WING|WOOD|XI|YOUNG|ZETA`)

var listCityStateZip []CityStateZip

var ps *pubsub.Client
var topic *pubsub.Topic
var topic2 *pubsub.Topic
var ap http.Client

var MLLabels map[string]string

func init() {
	ctx := context.Background()
	ps, _ = pubsub.NewClient(ctx, ProjectID)
	topic = ps.Topic(PubSubTopic)
	topic2 = ps.Topic(PubSubTopic2)
	MLLabels = map[string]string{"0": "", "1": "AD1", "2": "AD2", "3": "CITY", "4": "COUNTRY", "5": "EMAIL", "6": "FNAME", "7": "LNAME", "8": "PHONE", "9": "STATE", "10": "ZIP"}
	sClient, _ := storage.NewClient(ctx)
	listCityStateZip, _ = readCityStateZip(ctx, sClient, StorageBucket, "data/zip_city_state.json")
	ap = http.Client{
		Timeout: time.Second * 2, // Maximum of 2 secs
	}

	log.Printf("init completed, pubsub topic name: %v", topic)
}

func PostProcessPeople(ctx context.Context, m PubSubMessage) error {
	var input Input
	if err := json.Unmarshal(m.Data, &input); err != nil {
		log.Fatalf("Unable to unmarshal message %v with error %v", string(m.Data), err)
	}

	// assign match keys
	var mkOutput PeopleOutput
	var trustedID string
	var ClassYear string
	var dormERR bool
	var dormVER bool
	var dormAD1 string
	var dormColumn string
	var roomColumn string
	for index, column := range input.Columns {
		predictionValue := input.Prediction.Predictions[index]
		predictionKey := strconv.Itoa(int(predictionValue))

		matchKey := MLLabels[predictionKey]
		// log.Printf("column %v index %v prediction value %v formatted %v label %v", column, index, predictionValue, predictionKey, matchKey)
		column.MatchKey = matchKey

		// corrects the situation where FR is identified as a country
		if column.PeopleERR.Title == 1 && matchKey == "COUNTRY" {
			column.MatchKey = ""
		}

		// fix zip code that has leading 0 stripped out
		if matchKey == "ZIP" && IsInt(column.Value) && len(column.Value) < 5 {
			column.Value = LeftPad2Len(column.Value, "0", 5)
		}

		if matchKey != "" {
			// if it does not already have a value
			if len(GetMkField(&mkOutput, matchKey).Value) == 0 {
				SetMkField(&mkOutput, matchKey, column.Value, column.Name)
			}
		}

		// assign type if AD1
		if matchKey == "AD1" {
			mkOutput.AD1.Type = AssignAddressType(&column)
		}

		if column.PeopleERR.TrustedID == 1 {
			trustedID = column.Value
			SetMkField(&mkOutput, "CLIENTID", trustedID, column.Name)
		}

		if matchKey == "" && column.PeopleERR.Title == 1 && len(column.Value) > 0 {
			if reGraduationYear.MatchString(column.Value) {
				ClassYear = column.Value
			} else {
				switch strings.ToLower(column.Value) {
				case "freshman", "frosh", "fresh", "fr":
					ClassYear = strconv.Itoa(time.Now().Year() + 4)
				case "sophomore", "soph", "so":
					ClassYear = strconv.Itoa(time.Now().Year() + 3)
				case "junior", "jr":
					ClassYear = strconv.Itoa(time.Now().Year() + 2)
				case "senior", "sr":
					ClassYear = strconv.Itoa(time.Now().Year() + 1)
				default:
					ClassYear = strconv.Itoa(time.Now().Year() + 4)
				}
			}

			SetMkField(&mkOutput, "TITLE", ClassYear, column.Name)
		}

		// check for DORM address -- if we find Dorm ERR, Dorm VER, Room ERR then we have a dorm address
		if column.PeopleERR.Dorm == 1 {
			dormERR = true
			dormVER = reResidenceHall.MatchString(column.Value)
			if dormERR && dormVER {
				dormColumn = column.Name
				dormAD1 = column.Value
			}
		} else if column.PeopleERR.Room == 1 {
			roomColumn = column.Name
			if dormERR && dormVER && len(dormAD1) > 0 {
				dormAD1 += " " + column.Value

			}
		}
		input.Columns[index] = column

	}

	for _, column := range input.Columns {
		log.Printf("Column name %v value %v MatchKey %v VER %v ERR %v", column.Name, column.Value, column.MatchKey, column.PeopleVER, column.PeopleERR)
		if len(column.MatchKey) == 0 {
			if column.PeopleVER.IS_FIRSTNAME && len(GetMkField(&mkOutput, "FNAME").Value) == 0 && column.PeopleERR.Address1 == 0 && column.PeopleERR.City == 0 {
				mkOutput.FNAME.Value = column.Value
				mkOutput.FNAME.Source = column.Name
			} else if column.PeopleVER.IS_LASTNAME && len(GetMkField(&mkOutput, "LNAME").Value) == 0 && column.PeopleERR.Address1 == 0 && column.PeopleERR.City == 0 {
				mkOutput.LNAME.Value = column.Value
				mkOutput.LNAME.Source = column.Name
			} else if column.PeopleVER.IS_CITY && len(GetMkField(&mkOutput, "CITY").Value) == 0 && column.PeopleERR.FirstName == 0 && column.PeopleERR.LastName == 0 {
				mkOutput.CITY.Value = column.Value
				mkOutput.CITY.Source = column.Name
			} else if column.PeopleVER.IS_STATE && len(GetMkField(&mkOutput, "STATE").Value) == 0 {
				mkOutput.STATE.Value = column.Value
				mkOutput.STATE.Source = column.Name
			} else if column.PeopleVER.IS_ZIPCODE && len(GetMkField(&mkOutput, "ZIP").Value) == 0 {
				mkOutput.ZIP.Value = column.Value
				mkOutput.ZIP.Source = column.Name
			} else if column.PeopleVER.IS_STREET1 && len(GetMkField(&mkOutput, "AD1").Value) == 0 && column.PeopleERR.FirstName == 0 && column.PeopleERR.LastName == 0 {
				mkOutput.AD1.Value = column.Value
				mkOutput.AD1.Source = column.Name
				mkOutput.AD1.Type = AssignAddressType(&column)
			} else if column.PeopleVER.IS_EMAIL && len(GetMkField(&mkOutput, "EMAIL").Value) == 0 && column.PeopleERR.Role == 0 {
				mkOutput.EMAIL.Value = column.Value
				mkOutput.EMAIL.Source = column.Name
			} else if column.PeopleVER.IS_PHONE && len(GetMkField(&mkOutput, "PHONE").Value) == 0 && column.PeopleERR.Role == 0 && len(column.Value) >= 10 {
				numberValue := reNumberOnly.ReplaceAllString(column.Value, "")
				if len(numberValue) == 10 || (len(numberValue) == 11 && strings.HasPrefix(numberValue, "1")) {
					mkOutput.PHONE.Value = column.Value
					mkOutput.PHONE.Source = column.Name
				}
			}
		}
	}

	// parse address as needed
	addressInput := mkOutput.AD1.Value + " " + mkOutput.AD2.Value
	cityInput := mkOutput.CITY.Value
	if len(mkOutput.AD1.Value) > 0 && len(mkOutput.CITY.Value) == 0 && len(mkOutput.STATE.Value) == 0 && len(mkOutput.ZIP.Value) == 0 {
		a := ParseAddress(addressInput)
		log.Printf("address parser returned %v", a)
		if len(a.City) > 0 {
			mkOutput.CITY.Value = a.City
			mkOutput.CITY.Source = "Address Parser"
			mkOutput.STATE.Value = a.State
			mkOutput.STATE.Source = "Address Parser"
			mkOutput.ZIP.Value = a.Zip
			if len(a.Plus4) > 0 {
				mkOutput.ZIP.Value = a.Zip + "-" + a.Plus4
			}
			mkOutput.ZIP.Source = "Address Parser"
			mkOutput.AD1.Value = a.Number + " " + a.Street + " " + a.Type
			mkOutput.AD1.Source = "Address Parser"
			mkOutput.AD2.Value = a.SecUnitType + " " + a.SecUnitNum
			mkOutput.AD2.Source = "Address Parser"
		}
	} else if len(mkOutput.CITY.Value) > 0 && len(mkOutput.STATE.Value) == 0 && len(mkOutput.ZIP.Value) == 0 {
		a := ParseAddress("123 Main St, " + cityInput)
		log.Printf("address parser returned %v", a)
		if len(a.City) > 0 {
			mkOutput.CITY.Value = a.City
			mkOutput.CITY.Source = "Address Parser"
			mkOutput.STATE.Value = a.State
			mkOutput.STATE.Source = "Address Parser"
			mkOutput.ZIP.Value = a.Zip
			if len(a.Plus4) > 0 {
				mkOutput.ZIP.Value = a.Zip + "-" + a.Plus4
			}
			mkOutput.ZIP.Source = "Address Parser"
		}
	}

	// check zip city state match
	ZipCheck := CheckCityStateZip(mkOutput.CITY.Value, mkOutput.STATE.Value, mkOutput.ZIP.Value)
	// disabled
	if ZipCheck == false && len(mkOutput.AD1.Value) > 0 && false {
		address := strings.Join([]string{mkOutput.AD1.Value, mkOutput.AD2.Value, mkOutput.CITY.Value, mkOutput.STATE.Value, mkOutput.ZIP.Value}, ",")
		correctedOutputAddress := CorrectAddress(address)
		if len(correctedOutputAddress) > 0 {
			mkOutput.AD1.Value = strings.Join([]string{correctedOutputAddress[0].Components.PrimaryNumber, " ", correctedOutputAddress[0].Components.StreetPredirection, " ", correctedOutputAddress[0].Components.StreetName, " ", correctedOutputAddress[0].Components.StreetSuffix}, "")
			mkOutput.AD1.Source = "SmartyStreet"
			mkOutput.AD2.Value = strings.Join([]string{correctedOutputAddress[0].Components.SecondaryDesignator, " ", correctedOutputAddress[0].Components.SecondaryNumber}, "")
			mkOutput.AD2.Source = "SmartyStreet"
			mkOutput.CITY.Value = correctedOutputAddress[0].Components.CityName
			mkOutput.CITY.Source = "SmartyStreet"
			mkOutput.STATE.Value = correctedOutputAddress[0].Components.StateAbbreviation
			mkOutput.STATE.Source = "SmartyStreet"
			mkOutput.ZIP.Value = correctedOutputAddress[0].Components.Zipcode
			mkOutput.ZIP.Source = "SmartyStreet"
		}
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
			"type":   "people",
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
			"type":   "people",
			"source": "post",
		},
	})

	// multi-person
	var parents []MultiPersonRecord
	var fnames []string
	var fnameColumns []string
	var fnameParents []int
	var lnames []string
	var lnameColumns []string
	var lnameParents []int
	var emails []string
	var emailColumns []string
	var emailParents []int
	var mkFirstNameCount int
	var mkLastNameCount int
	var mkEmailCount int
	for _, column := range input.Columns {
		if column.MatchKey == "FNAME" {
			mkFirstNameCount++
			fnames = append(fnames, column.Value)
			fnameColumns = append(fnameColumns, column.Name)
			fnameParents = append(fnameParents, column.PeopleERR.Role)
		}
		if column.MatchKey == "LNAME" {
			mkLastNameCount++
			lnames = append(lnames, column.Value)
			lnameColumns = append(lnameColumns, column.Name)
			lnameParents = append(lnameParents, column.PeopleERR.Role)
		}
		if column.MatchKey == "EMAIL" {
			mkEmailCount++
			emails = append(emails, column.Value)
			emailColumns = append(emailColumns, column.Name)
			emailParents = append(emailParents, column.PeopleERR.Role)
		}
	}
	log.Printf("MPR fname count %v %v, lname count %v %v, email count %v %v", mkFirstNameCount, fnames, mkLastNameCount, lnames, mkEmailCount, emails)

	if mkFirstNameCount > 1 {
		// we have more than 1 person in the record, let's make some sets
		for index, fname := range fnames {
			if index > 0 && fnameParents[index] == 1 {
				parent := MultiPersonRecord{
					FNAME:       fname,
					FNAMEColumn: fnameColumns[index],
				}
				if len(lnames) > index && lnameParents[index] == 1 {
					parent.LNAME = lnames[index]
					parent.LNAMEColumn = lnameColumns[index]
				}

				if len(emails) > index && emailParents[index] == 1 {
					parent.EMAIL = emails[index]
					parent.EMAILColumn = emailColumns[index]
				} else {
					parent.EMAIL = ""
					parent.EMAILColumn = ""
				}
				parents = append(parents, parent)
			}
		}
	}

	log.Printf("MPR: %v", parents)
	if len(parents) > 0 {
		for i, parent := range parents {
			var parentOutput Output
			deepcopier.Copy(&output).To(&parentOutput)
			parentOutput.MatchKeys.FNAME = MatchKeyField{
				Value:  parent.FNAME,
				Source: parent.FNAMEColumn,
			}
			parentOutput.MatchKeys.LNAME = MatchKeyField{
				Value:  parent.LNAME,
				Source: parent.LNAMEColumn,
			}

			parentOutput.MatchKeys.EMAIL = MatchKeyField{
				Value:  parent.EMAIL,
				Source: parent.EMAILColumn,
			}

			parentOutput.MatchKeys.ROLE = MatchKeyField{
				Value:  "Parent",
				Source: "Post-Process",
			}

			parentOutput.Signature.RecordID += "-" + strconv.Itoa(i)
			// okay let's publish these
			parentJSON, _ := json.Marshal(parentOutput)

			log.Printf("output message %v", string(parentJSON))

			psresult := topic.Publish(ctx, &pubsub.Message{
				Data: parentJSON,
			})
			psid, err := psresult.Get(ctx)
			_, err = psresult.Get(ctx)
			if err != nil {
				log.Fatalf("%v Could not pub parent to pubsub: %v", input.Signature.EventID, err)
			} else {
				log.Printf("%v pubbed parent record as message id %v: %v", input.Signature.EventID, psid, string(parentJSON))
			}
		}
	}

	// detect non-MPR multi emails
	if mkEmailCount > mkFirstNameCount {
		for index, email := range emails {
			if emailParents[index] == 0 { // not a parent email
				var emailOutput Output
				deepcopier.Copy(&output).To(&emailOutput)

				emailOutput.MatchKeys.EMAIL = MatchKeyField{
					Value:  email,
					Source: emailColumns[index],
				}

				// okay let's publish these
				emailJSON, _ := json.Marshal(emailOutput)

				log.Printf("output message %v", string(emailJSON))

				psresult := topic.Publish(ctx, &pubsub.Message{
					Data: emailJSON,
				})
				psid, err := psresult.Get(ctx)
				_, err = psresult.Get(ctx)
				if err != nil {
					log.Fatalf("%v Could not pub email to pubsub: %v", input.Signature.EventID, err)
				} else {
					log.Printf("%v pubbed email record as message id %v: %v", input.Signature.EventID, psid, string(emailJSON))
				}
			}
		}
	}

	// see if we should pub a record with dorm address
	var dormOutput Output
	if len(dormAD1) > 0 {
		deepcopier.Copy(&output).To(&dormOutput)
		sourceColumn := dormColumn
		if len(roomColumn) > 0 {
			sourceColumn += "," + roomColumn
		}
		dormOutput.MatchKeys.AD1 = MatchKeyField{
			Value:  dormAD1,
			Type:   "Campus",
			Source: sourceColumn,
		}

		dormJSON, _ := json.Marshal(dormOutput)

		log.Printf("output message %v", string(dormJSON))

		psresult := topic.Publish(ctx, &pubsub.Message{
			Data: dormJSON,
		})
		psid, err := psresult.Get(ctx)
		_, err = psresult.Get(ctx)
		if err != nil {
			log.Fatalf("%v Could not pub dorm to pubsub: %v", input.Signature.EventID, err)
		} else {
			log.Printf("%v pubbed dorm record as message id %v: %v", input.Signature.EventID, psid, string(dormJSON))
		}
	}

	return nil
}

func CorrectAddress(in string) SmartyStreetResponse {
	var smartyStreetResponse SmartyStreetResponse
	smartyStreetRequestURL := fmt.Sprintf(SmartyStreetsEndpoint, url.QueryEscape(in))
	log.Printf("invoking smartystreet request %v", smartyStreetRequestURL)
	response, err := http.Get(smartyStreetRequestURL)
	if err != nil {
		log.Fatalf("smartystreet request failed: %v", err)
	} else {
		if response.StatusCode != 200 {
			log.Fatalf("smartystreet request failed, status code:%v", response.StatusCode)
		}
		data, err := ioutil.ReadAll(response.Body)
		if err != nil {
			log.Fatalf("Couldn't read the smartystreet response: %v", err)
		}
		log.Printf("smartystreet response %v", string(data))
		json.Unmarshal(data, &smartyStreetResponse)

		if len(smartyStreetResponse) > 0 {
			// correctedAddress.Add1 = smartyStreetResponse[0].DeliveryLine1
			// correctedAddress.Add2 = strings.Join([]string{smartyStreetResponse[0].Components.SecondaryDesignator, " ", smartyStreetResponse[0].Components.SecondaryNumber}, "")
			// if len(strings.TrimSpace(correctedAddress.Add2)) == 0 {
			// 	correctedAddress.Add2 = ""
			// }
			// correctedAddress.City = smartyStreetResponse[0].Components.CityName
			// correctedAddress.State = smartyStreetResponse[0].Components.StateAbbreviation
			// correctedAddress.Postal = smartyStreetResponse[0].Components.Zipcode
			// if len(smartyStreetResponse[0].Components.Plus4Code) > 0 {
			// 	correctedAddress.Postal = strings.Join([]string{smartyStreetResponse[0].Components.Zipcode, "-", smartyStreetResponse[0].Components.Plus4Code}, "")
			// }
			// correctedAddress.CityStateZipMatch = true
			// correctedAddress.Lat = smartyStreetResponse[0].Metadata.Latitude
			// correctedAddress.Long = smartyStreetResponse[0].Metadata.Longitude
			// correctedAddress.Number = smartyStreetResponse[0].Components.PrimaryNumber
			// correctedAddress.Directional = smartyStreetResponse[0].Components.StreetPredirection
			// correctedAddress.StreetName = smartyStreetResponse[0].Components.StreetName
			// correctedAddress.PostType = smartyStreetResponse[0].Components.StreetSuffix

			// correctedAddress.OccupancyType = smartyStreetResponse[0].Components.SecondaryDesignator
			// correctedAddress.OccupancyIdentifier = smartyStreetResponse[0].Components.SecondaryNumber

			// correctedAddress.MailRoute = smartyStreetResponse[0].Metadata.CarrierRoute
			return smartyStreetResponse
		}
	}
	return nil
}

func IsInt(s string) bool {
	for _, c := range s {
		if !unicode.IsDigit(c) {
			return false
		}
	}
	return true
}

func LeftPad2Len(s string, padStr string, overallLen int) string {
	var padCountInt = 1 + ((overallLen - len(padStr)) / len(padStr))
	var retStr = strings.Repeat(padStr, padCountInt) + s
	return retStr[(len(retStr) - overallLen):]
}

func GetMkField(v *PeopleOutput, field string) MatchKeyField {
	r := reflect.ValueOf(v)
	f := reflect.Indirect(r).FieldByName(field)
	return f.Interface().(MatchKeyField)
}

func SetMkField(v *PeopleOutput, field string, value string, source string) string {
	r := reflect.ValueOf(v)
	f := reflect.Indirect(r).FieldByName(field)

	f.Set(reflect.ValueOf(MatchKeyField{Value: value, Source: source}))
	return value
}

func CheckCityStateZip(city string, state string, zip string) bool {
	checkZip := zip
	if len(checkZip) > 5 {
		checkZip = checkZip[0:5]
	}
	checkCity := strings.TrimSpace(strings.ToLower(city))
	checkState := strings.TrimSpace(strings.ToLower(state))
	var result bool
	result = false

	// TODO: store this in binary search tree or something
	for _, item := range listCityStateZip {
		if IndexOf(checkCity, item.Cities) > -1 && checkState == item.State && checkZip == item.Zip {
			return true
		}
	}
	return result
}

func readCityStateZip(ctx context.Context, client *storage.Client, bucket, object string) ([]CityStateZip, error) {
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

func IndexOf(element string, data []string) int {
	for k, v := range data {
		if element == v {
			return k
		}
	}
	return -1 //not found.
}

func ParseAddress(address string) AddressParsed {
	req, err := http.NewRequest(http.MethodGet, AddressParserEndpoint, nil)
	if err != nil {
		log.Fatalf("error preparing address parser: %v", err)
	}
	req.URL.Query().Add("a", address)

	res, getErr := ap.Do(req)
	if getErr != nil {
		log.Fatalf("error calling address parser: %v", getErr)
	}

	body, readErr := ioutil.ReadAll(res.Body)
	if readErr != nil {
		log.Fatalf("error reading address parser response: %v", readErr)
	}

	var parsed AddressParsed
	jsonErr := json.Unmarshal(body, &parsed)
	if jsonErr != nil {
		log.Fatalf("error parsing address parser response: %v", jsonErr)
	}
	return parsed
}

func AssignAddressType(column *InputColumn) string {
	if column.PeopleERR.AddressTypeBilling == 1 {
		return "Billing"
	} else if column.PeopleERR.AddressTypeShipping == 1 {
		return "Shipping"
	} else if column.PeopleERR.AddressTypeHome == 1 {
		return "Home"
	} else if column.PeopleERR.AddressTypeCampus == 1 {
		return "Campus"
	}
	return ""
}
