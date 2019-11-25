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
	// "github.com/ulule/deepcopier"
	// box
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
	NER       NER       `json:"NER"`
	PeopleERR PeopleERR `json:"PeopleERR"`
	PeopleVER PeopleVER `json:"VER"`
	Name      string    `json:"Name"`
	Value     string    `json:"Value"`
	Type      string    `json:"Type"`
	MatchKey  string    `json:"MK"`  // model match key
	MatchKey1 string    `json:"MK1"` // assigned key 1
	MatchKey2 string    `json:"MK2"` // assigned key 2
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
	SALUTATION MatchKeyField `json:"salutation" bigquery:"salutation"`
	NICKNAME   MatchKeyField `json:"nickname" bigquery:"nickname"`
	FNAME      MatchKeyField `json:"fname" bigquery:"fname"`
	FINITIAL   MatchKeyField `json:"finitial" bigquery:"finitial"`
	LNAME      MatchKeyField `json:"lname" bigquery:"lname"`
	FULLNAME   MatchKeyField `json:"-" bigquery:"-"` // do not output in json or store in BQ

	AD1          MatchKeyField `json:"ad1" bigquery:"ad1"`
	AD1NO        MatchKeyField `json:"ad1no" bigquery:"ad1no"`
	AD2          MatchKeyField `json:"ad2" bigquery:"ad2"`
	AD3          MatchKeyField `json:"ad3" bigquery:"ad3"`
	CITY         MatchKeyField `json:"city" bigquery:"city"`
	STATE        MatchKeyField `json:"state" bigquery:"state"`
	ZIP          MatchKeyField `json:"zip" bigquery:"zip"`
	ZIP5         MatchKeyField `json:"zip5" bigquery:"zip5"`
	COUNTRY      MatchKeyField `json:"country" bigquery:"country"`
	MAILROUTE    MatchKeyField `json:"mailroute" bigquery:"mailroute"`
	ADTYPE       MatchKeyField `json:"adtype" bigquery:"adtype"`
	ADPARSER     MatchKeyField `json:"adparser" bigquery:"adparser"`
	ADCORRECT    MatchKeyField `json:"adcorrect" bigquery:"adcorrect"`
	DORM         MatchKeyField `json:"-" bigquery:"-"` // do not output in json or store in BQ
	ROOM         MatchKeyField `json:"-" bigquery:"-"` // do not output in json or store in BQ
	FULLADDRESS  MatchKeyField `json:"-" bigquery:"-"` // do not output in json or store in BQ
	CITYSTATEZIP MatchKeyField `json:"-" bigquery:"-"` // do not output in json or store in BQ

	EMAIL MatchKeyField `json:"email" bigquery:"email"`
	PHONE MatchKeyField `json:"phone" bigquery:"phone"`

	TRUSTEDID MatchKeyField `json:"trustedId" bigquery:"trustedid"`
	CLIENTID  MatchKeyField `json:"clientId" bigquery:"clientid"`

	GENDER MatchKeyField `json:"gender" bigquery:"gender"`
	AGE    MatchKeyField `json:"age" bigquery:"age"`
	DOB    MatchKeyField `json:"dob" bigquery:"dob"`

	ORGANIZATION MatchKeyField `json:"organization" bigquery:"organization"`
	TITLE        MatchKeyField `json:"title" bigquery:"title"`
	ROLE         MatchKeyField `json:"role" bigquery:"role"`
	STATUS       MatchKeyField `json:"status" bigquery:"status"`
}

type PeopleERR struct {
	Address             int `json:"Address"`
	Address1            int `json:"Address1"`
	Address2            int `json:"Address2"`
	Address3            int `json:"Address3"`
	FullAddress         int `json:"FullAddress"`
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
	ContainsFirstName   int `json:"ContainsFirstName"`
	ContainsName        int `json:"ContainsName"`
	ContainsLastName    int `json:"ContainsLastName"`
	ContainsCountry     int `json:"ContainsCountry"`
	ContainsEmail       int `json:"ContainsEmail"`
	ContainsAddress     int `json:"ContainsAddress"`
	ContainsCity        int `json:"ContainsCity"`
	ContainsState       int `json:"ContainsState"`
	ContainsZipCode     int `json:"ContainsZipCode"`
	ContainsPhone       int `json:"ContainsPhone"`
	ContainsTitle       int `json:"ContainsTitle"`
	ContainsRole        int `json:"ContainsRole"`
	ContainsStudentRole int `json:"ContainsStudentRole"`
	Junk                int `json:"Junk"`
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

type LibPostal struct {
	Label string `json:"label"`
	Value string `json:"value"`
}

type CityStateZip struct {
	Cities []string `json:"cities"`
	State  string   `json:"state"`
	Zip    string   `json:"zip"`
}

type LibPostalParsed struct {
	HOUSE          string
	CATEGORY       string
	NEAR           string
	HOUSE_NUMBER   string
	ROAD           string
	UNIT           string
	LEVEL          string
	STAIRCASE      string
	ENTRANCE       string
	PO_BOX         string
	POSTCODE       string
	SUBURB         string
	CITY_DISTRICT  string
	CITY           string
	ISLAND         string
	STATE_DISTRICT string
	STATE          string
	COUNTRY_REGION string
	COUNTRY        string
	WORLD_REGION   string
}

type NameParsed struct {
	FNAME  string
	LNAME  string
	SUFFIX string
}

type PostRecord struct {
	Type     string
	Sequence int
	Output   PeopleOutput
}

var ProjectID = os.Getenv("PROJECTID")
var PubSubTopic = os.Getenv("PSOUTPUT")
var dev = os.Getenv("ENVIRONMENT") == "dev"

// var PubSubTopic2 = os.Getenv("PSOUTPUT2")  // why would we pub SAME thing twice?

var SmartyStreetsEndpoint = os.Getenv("SMARTYSTREET")
var AddressParserBaseUrl = os.Getenv("ADDRESSURL")
var AddressParserPath = os.Getenv("ADDRESSPATH")

var StorageBucket = os.Getenv("CLOUDSTORAGE")

var reGraduationYear = regexp.MustCompile(`20^\d{2}$`)
var reNumberOnly = regexp.MustCompile("[^0-9]+")
var reConcatenatedAddress = regexp.MustCompile(`(\d*)\s+((?:[\w+\s*\-])+)[\,]\s+([a-zA-Z]+)\s+([0-9a-zA-Z]+)`)
var reConcatenatedCityStateZip = regexp.MustCompile(`((?:[\w+\s*\-])+)[\,]\s+([a-zA-Z]+)\s+([0-9a-zA-Z]+)`)
var reNewline = regexp.MustCompile(`\r?\n`)
var reResidenceHall = regexp.MustCompile(`(?i)\sALPHA|ALUMNI|APARTMENT|APTS|BETA|BUILDING|CAMPUS|CENTENNIAL|CENTER|CHI|COLLEGE|COMMON|COMMUNITY|COMPLEX|COURT|CROSS|DELTA|DORM|EPSILON|ETA|FOUNDER|FOUNTAIN|FRATERNITY|GAMMA|GARDEN|GREEK|HALL|HEIGHT|HERITAGE|HIGH|HILL|HOME|HONOR|HOUS|INN|INTERNATIONAL|IOTA|KAPPA|LAMBDA|LANDING|LEARNING|LIVING|LODGE|MEMORIAL|MU|NU|OMEGA|OMICRON|PARK|PHASE|PHI|PI|PLACE|PLAZA|PSI|RESIDEN|RHO|RIVER|SCHOLARSHIP|SIGMA|SQUARE|STATE|STUDENT|SUITE|TAU|TERRACE|THETA|TOWER|TRADITIONAL|UNIV|UNIVERSITY|UPSILON|VIEW|VILLAGE|VISTA|WING|WOOD|XI|YOUNG|ZETA`)
var reState = regexp.MustCompile(`(?i)^AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY|DC|PR$`)
var reFullName = regexp.MustCompile(`^(.+?) ([^\s,]+)(,? (?:[JS]r\.?|III?|IV))?$`)

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
	// topic2 = ps.Topic(PubSubTopic2)
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

	var MPRCounter int       // keep track of how many MPR we have
	var MARCounter int       // keep track of how many MAR we have
	var outputs []PostRecord // this contains all outputs with a type

	log.Printf("people-post for record: %v", input.Signature.RecordID)

	// iterate through every column on the input record to decide what the column is...
	for index, column := range input.Columns {
		// start with some sanitization
		column.Value = strings.TrimSpace(column.Value)
		column.Value = reNewline.ReplaceAllString(column.Value, " ")
		column.Value = strings.Replace(column.Value, "  ", " ", -1)
		column.Value = strings.Replace(column.Value, "  ", " ", -1)
		if len(column.Value) == 0 { //dont need to work with blank values
			continue
		}

		// capture ML prediction to column
		predictionValue := input.Prediction.Predictions[index]
		predictionKey := strconv.Itoa(int(predictionValue))
		mlMatchKey := MLLabels[predictionKey]
		column.MatchKey = mlMatchKey

		// let's figure out which column this goes to
		if column.PeopleERR.TrustedID == 1 {
			column.MatchKey1 = "CLIENTID"
			LogDev(fmt.Sprintf("MatchKey %v on condition %v", column.MatchKey1, "column.PeopleERR.TrustedID == 1"))
		} else if column.PeopleERR.Organization == 1 {
			column.MatchKey1 = "ORGANIZATION"
			LogDev(fmt.Sprintf("MatchKey %v on condition %v", column.MatchKey1, "column.PeopleERR.Organization == 1"))
		} else if column.PeopleERR.Gender == 1 {
			column.MatchKey1 = "GENDER"
			LogDev(fmt.Sprintf("MatchKey %v on condition %v", column.MatchKey1, "column.PeopleERR.Gender == 1"))
		} else if column.PeopleERR.ContainsStudentRole == 1 {
			column.MatchKey1 = "ROLE"
			LogDev(fmt.Sprintf("MatchKey %v on condition %v", column.MatchKey1, "column.PeopleERR.ContainsStudentRole == 1"))
		} else if column.PeopleERR.Title == 1 || column.PeopleERR.ContainsTitle == 1 {
			column.MatchKey1 = "TITLE"
			column.MatchKey2 = "STATUS"
			LogDev(fmt.Sprintf("MatchKey %v on condition %v", column.MatchKey1, "column.PeopleERR.Title == 1 || column.PeopleERR.ContainsTitle == 1"))
			LogDev(fmt.Sprintf("MatchKey %v on condition %v", column.MatchKey2, "column.PeopleERR.Title == 1 || column.PeopleERR.ContainsTitle == 1"))
			column.MatchKey = ""
			column.PeopleERR.Country = 0 // override this is NOT a country
			column.PeopleERR.State = 0   // override this is NOT a state value
		} else if column.PeopleERR.Dorm == 1 && reResidenceHall.MatchString(column.Value) {
			column.MatchKey1 = "DORM"
			LogDev(fmt.Sprintf("MatchKey %v on condition %v", column.MatchKey1, "column.PeopleERR.Dorm == 1 && reResidenceHall.MatchString(column.Value)"))
		} else if column.PeopleERR.Room == 1 {
			column.MatchKey1 = "ROOM"
			LogDev(fmt.Sprintf("MatchKey %v on condition %v", column.MatchKey1, "column.PeopleERR.Room == 1"))
		} else if column.PeopleERR.FullAddress == 1 {
			column.MatchKey1 = "FULLADDRESS"
			LogDev(fmt.Sprintf("MatchKey %v on condition %v", column.MatchKey1, "column.PeopleERR.FullAddress == 1"))
		} else if column.PeopleERR.ContainsState == 1 && column.PeopleERR.ContainsCity == 1 {
			column.MatchKey1 = "CITYSTATEZIP"
			LogDev(fmt.Sprintf("MatchKey %v on condition %v", column.MatchKey1, "column.PeopleERR.ContainsState == 1 && column.PeopleERR.ContainsCity == 1"))
		}

		var parsedName NameParsed
		if column.PeopleERR.ContainsRole == 1 || column.PeopleERR.FullName == 1 || (column.PeopleVER.IS_FIRSTNAME && column.PeopleVER.IS_LASTNAME && ((column.PeopleERR.ContainsFirstName == 1 && column.PeopleERR.ContainsLastName == 1) || (column.PeopleERR.ContainsFirstName == 0 && column.PeopleERR.ContainsLastName == 0))) {
			// this might be a full name, try to parse it and see if we have first and last names
			parsedName = ParseName(column.Value)
			if len(parsedName.FNAME) > 0 && len(parsedName.LNAME) > 0 {
				column.MatchKey1 = "FULLNAME"
				LogDev(fmt.Sprintf("MatchKey %v on condition %v", column.MatchKey1, "len(parsedName.FNAME) > 0 && len(parsedName.LNAME) > 0"))
			}
		}
		if len(column.MatchKey1) == 0 {
			if column.PeopleVER.IS_FIRSTNAME && column.PeopleERR.FirstName == 1 {
				column.MatchKey1 = "FNAME"
				LogDev(fmt.Sprintf("MatchKey %v on condition %v", column.MatchKey1, "column.PeopleVER.IS_FIRSTNAME && column.PeopleERR.FirstName == 1"))
			} else if column.PeopleVER.IS_LASTNAME && column.PeopleERR.LastName == 1 {
				column.MatchKey1 = "LNAME"
				LogDev(fmt.Sprintf("MatchKey %v on condition %v", column.MatchKey1, "column.PeopleVER.IS_LASTNAME && column.PeopleERR.LastName == 1"))
			} else if column.PeopleVER.IS_STREET1 && column.PeopleERR.Address1 == 1 {
				column.MatchKey1 = "AD1"
				LogDev(fmt.Sprintf("MatchKey %v on condition %v", column.MatchKey1, "column.PeopleVER.IS_STREET1 && column.PeopleERR.Address1 == 1"))
			} else if column.PeopleVER.IS_STREET2 && column.PeopleERR.Address2 == 1 {
				column.MatchKey1 = "AD2"
				LogDev(fmt.Sprintf("MatchKey %v on condition %v", column.MatchKey1, "column.PeopleVER.IS_STREET2 && column.PeopleERR.Address2 == 1"))
			} else if column.PeopleVER.IS_STREET3 && column.PeopleERR.Address3 == 1 {
				column.MatchKey1 = "AD3"
				LogDev(fmt.Sprintf("MatchKey %v on condition %v", column.MatchKey1, "column.PeopleVER.IS_STREET3 && column.PeopleERR.Address3 == 1"))
			} else if column.PeopleVER.IS_CITY && column.PeopleERR.City == 1 {
				column.MatchKey1 = "CITY"
				LogDev(fmt.Sprintf("MatchKey %v on condition %v", column.MatchKey1, "column.PeopleVER.IS_CITY && column.PeopleERR.City == 1"))
			} else if column.PeopleVER.IS_STATE && column.PeopleERR.State == 1 {
				column.MatchKey1 = "STATE"
				LogDev(fmt.Sprintf("MatchKey %v on condition %v", column.MatchKey1, "column.PeopleVER.IS_STATE && column.PeopleERR.State == 1"))
			} else if column.PeopleVER.IS_ZIPCODE && column.PeopleERR.ZipCode == 1 {
				column.MatchKey1 = "ZIP"
				LogDev(fmt.Sprintf("MatchKey %v on condition %v", column.MatchKey1, "column.PeopleVER.IS_ZIPCODE && column.PeopleERR.ZipCode == 1"))
			} else if column.PeopleVER.IS_COUNTRY && column.PeopleERR.Country == 1 {
				column.MatchKey1 = "COUNTRY"
				LogDev(fmt.Sprintf("MatchKey %v on condition %v", column.MatchKey1, "column.PeopleVER.IS_COUNTRY && column.PeopleERR.Country == 1"))
			} else if column.PeopleVER.IS_EMAIL {
				column.MatchKey1 = "EMAIL"
				LogDev(fmt.Sprintf("MatchKey %v on condition %v", column.MatchKey1, "column.PeopleVER.IS_EMAIL"))
			} else if column.PeopleVER.IS_PHONE && len(column.Value) >= 10 {
				numberValue := reNumberOnly.ReplaceAllString(column.Value, "")
				if len(numberValue) == 10 || (len(numberValue) == 11 && strings.HasPrefix(numberValue, "1")) { // only handle US phone format
					column.MatchKey1 = "PHONE"
					LogDev(fmt.Sprintf("MatchKey %v on condition %v", column.MatchKey1, "column.PeopleVER.IS_PHONE && len(column.Value) >= 10"))
				}
			} else if column.PeopleERR.ContainsFirstName == 1 && column.PeopleVER.IS_FIRSTNAME {
				column.MatchKey1 = "FNAME"
				LogDev(fmt.Sprintf("MatchKey %v on condition %v", column.MatchKey1, "column.PeopleERR.ContainsFirstName == 1 && column.PeopleVER.IS_FIRSTNAME"))
			} else if column.PeopleERR.FirstName == 1 {
				column.MatchKey1 = "FNAME"
				LogDev(fmt.Sprintf("MatchKey %v on condition %v", column.MatchKey1, "column.PeopleERR.FirstName == 1"))
			} else if column.PeopleERR.ContainsLastName == 1 && column.PeopleVER.IS_LASTNAME {
				column.MatchKey1 = "LNAME"
				LogDev(fmt.Sprintf("MatchKey %v on condition %v", column.MatchKey1, "column.PeopleERR.ContainsLastName == 1 && column.PeopleVER.IS_LASTNAME"))
			} else if column.PeopleERR.LastName == 1 {
				column.MatchKey1 = "LNAME"
				LogDev(fmt.Sprintf("MatchKey %v on condition %v", column.MatchKey1, "column.PeopleERR.LastName == 1"))
			} else if column.PeopleERR.Address1 == 1 {
				column.MatchKey1 = "AD1"
				LogDev(fmt.Sprintf("MatchKey %v on condition %v", column.MatchKey1, "column.PeopleERR.Address1 == 1"))
			} else if column.PeopleERR.Address2 == 1 {
				column.MatchKey1 = "AD2"
				LogDev(fmt.Sprintf("MatchKey %v on condition %v", column.MatchKey1, "column.PeopleERR.Address2 == 1"))
			} else if column.PeopleERR.Address3 == 1 {
				column.MatchKey1 = "AD3"
				LogDev(fmt.Sprintf("MatchKey %v on condition %v", column.MatchKey1, "column.PeopleERR.Address3 == 1"))
			} else if column.PeopleERR.City == 1 {
				column.MatchKey1 = "CITY"
				LogDev(fmt.Sprintf("MatchKey %v on condition %v", column.MatchKey1, "column.PeopleERR.City == 1"))
			} else if column.PeopleERR.State == 1 {
				column.MatchKey1 = "STATE"
				LogDev(fmt.Sprintf("MatchKey %v on condition %v", column.MatchKey1, "column.PeopleERR.State == 1"))
			} else if column.PeopleERR.ZipCode == 1 {
				column.MatchKey1 = "ZIP"
				LogDev(fmt.Sprintf("MatchKey %v on condition %v", column.MatchKey1, "column.PeopleERR.ZipCode == 1"))
			} else if column.PeopleVER.IS_STREET1 && column.PeopleERR.Junk == 0 {
				column.MatchKey1 = "AD1"
				LogDev(fmt.Sprintf("MatchKey %v on condition %v", column.MatchKey1, "column.PeopleVER.IS_STREET1 && column.PeopleERR.Junk == 0"))
			} else if column.PeopleVER.IS_STREET2 && column.PeopleERR.Junk == 0 {
				column.MatchKey1 = "AD2"
				LogDev(fmt.Sprintf("MatchKey %v on condition %v", column.MatchKey1, "column.PeopleVER.IS_STREET2 && column.PeopleERR.Junk == 0"))
			} else if column.PeopleVER.IS_STREET3 && column.PeopleERR.Junk == 0 {
				column.MatchKey1 = "AD3"
				LogDev(fmt.Sprintf("MatchKey %v on condition %v", column.MatchKey1, "column.PeopleVER.IS_STREET3 && column.PeopleERR.Junk == 0"))
			} else if column.PeopleVER.IS_CITY && column.PeopleERR.Junk == 0 && column.PeopleERR.ContainsFirstName == 0 && column.PeopleERR.ContainsLastName == 0 && column.PeopleERR.MiddleName == 0 && column.PeopleERR.Gender == 0 {
				column.MatchKey1 = "CITY"
				LogDev(fmt.Sprintf("MatchKey %v on condition %v", column.MatchKey1, "column.PeopleVER.IS_CITY && column.PeopleERR.Junk == 0 && column.PeopleERR.ContainsFirstName == 0 && column.PeopleERR.ContainsLastName == 0 && column.PeopleERR.MiddleName == 0 && column.PeopleERR.Gender == 0"))
			} else if column.PeopleVER.IS_STATE && column.PeopleERR.Junk == 0 && column.PeopleERR.MiddleName == 0 {
				column.MatchKey1 = "STATE"
				LogDev(fmt.Sprintf("MatchKey %v on condition %v", column.MatchKey1, "column.PeopleVER.IS_STATE && column.PeopleERR.Junk == 0 && column.PeopleERR.MiddleName == 0"))
			} else if column.PeopleVER.IS_ZIPCODE && column.PeopleERR.ContainsZipCode == 1 && column.PeopleERR.Junk == 0 {
				column.MatchKey1 = "ZIP"
				LogDev(fmt.Sprintf("MatchKey %v on condition %v", column.MatchKey1, "column.PeopleVER.IS_ZIPCODE && column.PeopleERR.ContainsZipCode == 1 && column.PeopleERR.Junk == 0"))
			} else if column.PeopleVER.IS_COUNTRY {
				column.MatchKey1 = "COUNTRY"
				LogDev(fmt.Sprintf("MatchKey %v on condition %v", column.MatchKey1, "column.PeopleVER.IS_COUNTRY"))
			} else if column.PeopleERR.ContainsFirstName == 1 {
				column.MatchKey1 = "FNAME"
				LogDev(fmt.Sprintf("MatchKey %v on condition %v", column.MatchKey1, "column.PeopleERR.ContainsFirstName == 1"))
			} else if column.PeopleERR.ContainsLastName == 1 {
				column.MatchKey1 = "LNAME"
				LogDev(fmt.Sprintf("MatchKey %v on condition %v", column.MatchKey1, "column.PeopleERR.ContainsLastName == 1"))
			} else if column.PeopleERR.ContainsAddress == 1 {
				column.MatchKey1 = "AD1"
				LogDev(fmt.Sprintf("MatchKey %v on condition %v", column.MatchKey1, "column.PeopleERR.ContainsAddress == 1"))
			}
		}

		// fix zip code that has leading 0 stripped out
		if column.MatchKey1 == "ZIP" && IsInt(column.Value) && len(column.Value) < 5 {
			column.Value = LeftPad2Len(column.Value, "0", 5)
		}

		// type email if ends with gmail, yahoo, hotmail
		if column.MatchKey1 == "EMAIL" && len(column.Value) > 0 {
			email := strings.ToLower(column.Value)
			if strings.HasSuffix(email, "gmail.com") || strings.HasSuffix(email, "yahoo.com") || strings.HasSuffix(email, "hotmail.com") || strings.HasSuffix(email, "msn.com") || strings.HasSuffix(email, "aol.com") || strings.HasSuffix(email, "comcast.net") {
				column.Type = "Private"
			}
		}

		// AD type
		if column.MatchKey1 == "AD1" || column.MatchKey == "AD1" {
			column.Type = AssignAddressType(&column)
			column.MatchKey2 = "ADTYPE"
		}

		// clear MatchKey if Junk
		if column.PeopleERR.Junk == 1 {
			column.MatchKey = ""
		}

		// now that we have finished assignment, let's assign the columns, attempting to set value on a match key field that already contains a value will result in additional output being created
		matchKeyAssigned := ""
		if len(column.MatchKey1) > 0 {
			matchKeyAssigned = column.MatchKey1
			LogDev(fmt.Sprintf("matchkey assigned is %v from rules", matchKeyAssigned))
		} else if len(column.MatchKey) > 0 { // use the model default
			matchKeyAssigned = column.MatchKey
			LogDev(fmt.Sprintf("matchkey assigned is %v from prediction", matchKeyAssigned))
		}

		var currentOutput *PostRecord
		var indexOutput int
		if len(matchKeyAssigned) > 0 {
			if column.PeopleERR.ContainsRole == 0 { // not MPR
				currentOutput, indexOutput = GetOutputByType(&outputs, "default")
				if matchKeyAssigned == "DORM" || matchKeyAssigned == "ROOM" { // write out dorm address as a new output
					currentOutput, indexOutput = GetOutputByType(&outputs, "dorm")
				}
				currentValue := GetMkField(&(currentOutput.Output), matchKeyAssigned)
				for {
					if len(currentValue.Value) == 0 {
						break
					}
					MARCounter++
					currentOutput, indexOutput = GetOutputByTypeAndSequence(&outputs, "mar", MARCounter)
					currentValue = GetMkField(&(currentOutput.Output), matchKeyAssigned)
				}
			} else { // MPR
				mprExtracted := ExtractMPRCounter(column.Name)
				if MPRCounter == 0 {
					MPRCounter++
				}
				// how should this counter be used
				if mprExtracted > 0 {
					currentOutput, indexOutput = GetOutputByTypeAndSequence(&outputs, "mpr", mprExtracted)
				} else {
					currentOutput, indexOutput = GetOutputByTypeAndSequence(&outputs, "mpr", MPRCounter)
					currentValue := GetMkField(&(currentOutput.Output), matchKeyAssigned)
					for {
						if len(currentValue.Value) == 0 {
							break
						}
						MPRCounter++
						currentOutput, indexOutput = GetOutputByTypeAndSequence(&outputs, "mpr", MPRCounter)
						currentValue = GetMkField(&(currentOutput.Output), matchKeyAssigned)
					}
				}
			}

			// let's assign the value
			switch matchKeyAssigned {
			case "TITLE":
				SetMkField(&(currentOutput.Output), "TITLE", CalcClassYear(column.Value), column.Name)
			case "FULLNAME":
				SetMkField(&(currentOutput.Output), "FULLNAME", column.Value, column.Name)
				if len(parsedName.FNAME) > 0 && len(parsedName.LNAME) > 0 {
					SetMkField(&(currentOutput.Output), "FNAME", parsedName.FNAME, column.Name)
					SetMkField(&(currentOutput.Output), "LNAME", parsedName.LNAME, column.Name)
				}
			case "DORM":
				SetMkField(&(currentOutput.Output), "AD1", column.Value, column.Name)
				SetMkField(&(currentOutput.Output), "ADTYPE", "Campus", column.Name)
			case "ROOM":
				SetMkField(&(currentOutput.Output), "AD2", column.Name+": "+column.Value, column.Name)
			case "FULLADDRESS":
				SetMkField(&(currentOutput.Output), "AD1", column.Value, column.Name)
			case "CITYSTATEZIP":
				SetMkField(&(currentOutput.Output), "CITY", column.Value, column.Name)
			default:
				SetMkFieldWithType(&(currentOutput.Output), matchKeyAssigned, column.Value, column.Name, column.Type)
			}

			if len(column.MatchKey2) > 0 {
				switch column.MatchKey2 {
				case "STATUS":
					SetMkField(&(currentOutput.Output), "STATUS", CalcClassDesig(column.Value), column.Name)
				case "ADTYPE":
					SetMkField(&(currentOutput.Output), "ADTYPE", AssignAddressType(&column), column.Name)
				}
			}
			//columnOutput := *currentOutput
			outputs[indexOutput] = *currentOutput
		} else {
			log.Printf("Event %v Record %v Column has no match key assigned: : %v %v", input.Signature.EventID, input.Signature.RecordID, column.Name, column.Value)
		}
		LogDev(fmt.Sprintf("Outputs is %v", outputs))
		// input.Columns[index] = column // dont need to update the input
	}

	log.Printf("Finishing with %v outputs", len(outputs))

	for i, v := range outputs {
		// clean up the address
		ProcessAddress(&(v.Output))
		log.Printf("Pub output %v of %v, type %v, sequence %v: %v", i, len(outputs), v.Type, v.Sequence, v.Output)
		suffix := ""
		if v.Type == "mpr" {
			suffix = strconv.Itoa(v.Sequence)
		}
		PubRecord(ctx, &input, v.Output, suffix)
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

// I'm guessing what this does is record SOR >< MatchKey field mapping... for ABM
func SetMkField(v *PeopleOutput, field string, value string, source string) {
	r := reflect.ValueOf(v)
	f := reflect.Indirect(r).FieldByName(field)
	f.Set(reflect.ValueOf(MatchKeyField{Value: value, Source: source}))
	if dev {
		log.Printf("SetMkField: %v %v %v", field, value, source)
		log.Printf("MkField %v", GetMkField(v, field))
	}
}

func SetMkFieldWithType(v *PeopleOutput, field string, value string, source string, t string) {
	r := reflect.ValueOf(v)
	f := reflect.Indirect(r).FieldByName(field)

	f.Set(reflect.ValueOf(MatchKeyField{Value: value, Source: source, Type: t}))
	if dev {
		log.Printf("SetMkField: %v %v %v %v", field, value, source, t)
		log.Printf("MkField %v", GetMkField(v, field))
	}
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

func ProcessAddress(mkOutput *PeopleOutput) {
	addressInput := mkOutput.AD1.Value + " " + mkOutput.AD2.Value + ", " + mkOutput.CITY.Value + ", " + mkOutput.STATE.Value + " " + mkOutput.ZIP.Value + ", " + mkOutput.COUNTRY.Value
	if len(strings.TrimSpace(addressInput)) > 0 {
		a := ParseAddress(reNewline.ReplaceAllString(addressInput, ""))
		if dev {
			log.Printf("address parser returned %v from input %v", a, addressInput)
		}
		if len(a.CITY) > 0 {
			mkOutput.CITY.Value = strings.ToUpper(a.CITY)
			mkOutput.STATE.Value = strings.ToUpper(a.STATE)
			mkOutput.ZIP.Value = strings.ToUpper(a.POSTCODE)
			if len(a.COUNTRY) > 0 {
				mkOutput.COUNTRY.Value = strings.ToUpper(a.COUNTRY)
			}
			mkOutput.ADPARSER.Value = "libpostal"
			if len(a.PO_BOX) > 0 {
				if len(a.HOUSE_NUMBER) > 0 {
					mkOutput.AD1.Value = strings.ToUpper(a.HOUSE_NUMBER + " " + a.ROAD)
					mkOutput.AD1NO.Value = strings.ToUpper(a.HOUSE_NUMBER)
					mkOutput.AD2.Value = strings.ToUpper(a.PO_BOX)
				} else {
					mkOutput.AD1.Value = strings.ToUpper(a.PO_BOX)
					mkOutput.AD1NO.Value = strings.TrimPrefix(a.PO_BOX, "PO BOX ")
				}
			} else {
				mkOutput.AD1.Value = strings.ToUpper(a.HOUSE_NUMBER + " " + a.ROAD)
				mkOutput.AD1NO.Value = strings.ToUpper(a.HOUSE_NUMBER)
				mkOutput.AD2.Value = strings.ToUpper(a.LEVEL) + " " + strings.ToUpper(a.UNIT)
			}
			if reState.MatchString(a.STATE) {
				mkOutput.COUNTRY.Value = "US"
				mkOutput.COUNTRY.Source = "WM"
			}
		}
	}
}

// DEPRECATED, keeping for reference
func AddressParse(mko *PeopleOutput, input *Input, concatCityState bool, concatCityStateCol int, concatAdd bool, concatAddCol int) {
	var addressInput string

	if !concatCityState && !concatAdd {
		addressInput = mko.AD1.Value + " " + mko.AD2.Value + " " + mko.CITY.Value + " " + mko.STATE.Value + " " + mko.ZIP.Value
		if dev {
			log.Printf("!concatAdd + !concatCityState %v ", addressInput)
		}
	} else if !concatAdd && concatCityState {
		addressInput = mko.AD1.Value + " " + mko.AD2.Value + " " + input.Columns[concatCityStateCol].Value
		if dev {
			log.Printf("!concatAdd + concatCityState %v ", addressInput)
		}
	} else if concatAdd && !concatCityState {
		addressInput = input.Columns[concatAddCol].Value
		if dev {
			log.Printf("concatAdd + !concatCityState %v ", addressInput)
		}
	} else if concatAdd && concatCityState {
		// this is potentially duplicate data?
		addressInput = input.Columns[concatAddCol].Value + input.Columns[concatCityStateCol].Value
		if dev {
			log.Printf("concatAdd + concatCityState %v ", addressInput)
		}
	}

	if len(strings.TrimSpace(addressInput)) > 0 {
		a := ParseAddress(addressInput)
		log.Printf("address parser returned %v", a)
		if len(a.CITY) > 0 || len(a.CITY_DISTRICT) > 0 {
			if len(a.CITY) > 0 {
				mko.CITY.Value = strings.ToUpper(a.CITY)
			} else {
				mko.CITY.Value = strings.ToUpper(a.CITY_DISTRICT)
			}
			mko.STATE.Value = strings.ToUpper(a.STATE)
			mko.ZIP.Value = strings.ToUpper(a.POSTCODE)
			if len(a.COUNTRY) > 0 {
				mko.COUNTRY.Value = strings.ToUpper(a.COUNTRY)
			}
			mko.ADPARSER.Value = "libpostal"
			if len(a.PO_BOX) > 0 {
				if len(a.HOUSE_NUMBER) > 0 {
					mko.AD1.Value = strings.ToUpper(a.HOUSE_NUMBER + " " + a.ROAD + " " + a.SUBURB)
					mko.AD1NO.Value = strings.ToUpper(a.HOUSE_NUMBER)
					mko.AD2.Value = strings.ToUpper(a.PO_BOX)
				} else {
					mko.AD1.Value = strings.ToUpper(a.PO_BOX)
					mko.AD1NO.Value = strings.TrimPrefix(a.PO_BOX, "PO BOX ")
				}
			} else {
				mko.AD1.Value = strings.ToUpper(a.HOUSE_NUMBER + " " + a.ROAD)
				mko.AD1NO.Value = strings.ToUpper(a.HOUSE_NUMBER)
				mko.AD2.Value = strings.ToUpper(a.LEVEL) + " " + strings.ToUpper(a.UNIT)
			}
			if reState.MatchString(a.STATE) {
				SetMkField(mko, "COUNTRY", "US", "WM")
			}
		}
	}

}

func ParseAddress(address string) LibPostalParsed {
	baseUrl, err := url.Parse(AddressParserBaseUrl)
	baseUrl.Path += AddressParserPath
	params := url.Values{}
	params.Add("address", address)
	baseUrl.RawQuery = params.Encode()

	req, err := http.NewRequest(http.MethodGet, baseUrl.String(), nil)
	if err != nil {
		log.Fatalf("error preparing address parser: %v", err)
	}
	// req.URL.Query().Add("a", address)

	res, getErr := ap.Do(req)
	if getErr != nil {
		log.Fatalf("error calling address parser: %v", getErr)
	}

	body, readErr := ioutil.ReadAll(res.Body)
	if readErr != nil {
		log.Fatalf("error reading address parser response: %v", readErr)
	}

	var parsed []LibPostal
	jsonErr := json.Unmarshal(body, &parsed)
	if jsonErr != nil {
		log.Fatalf("error parsing address parser response: %v, body %v", jsonErr, string(body))
	} else {
		log.Printf("address parser reponse: %v", string(body))
	}

	var result LibPostalParsed
	for _, lp := range parsed {
		SetLibPostalField(&result, strings.ToUpper(lp.Label), lp.Value)
	}

	return result
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

func ExtractMPRCounter(columnName string) int {
	if strings.Contains(columnName, "first") || strings.Contains(columnName, "1") || strings.Contains(columnName, "father") {
		return 1
	}
	if strings.Contains(columnName, "second") || strings.Contains(columnName, "2") || strings.Contains(columnName, "mother") {
		return 2
	}
	if strings.Contains(columnName, "third") || strings.Contains(columnName, "3") {
		return 3
	}
	// if we don't find anything intersting, then return 0 and let the caller figure out
	return 0
}

func PubRecord(ctx context.Context, input *Input, mkOutput PeopleOutput, suffix string) {
	var output Output
	output.Signature = input.Signature
	if len(suffix) > 0 {
		output.Signature.RecordID += suffix
	}
	output.Passthrough = input.Passthrough

	output.MatchKeys = mkOutput

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
}

func SetLibPostalField(v *LibPostalParsed, field string, value string) string {
	r := reflect.ValueOf(v)
	f := reflect.Indirect(r).FieldByName(field)
	f.SetString(value)
	return value
}

func CalcClassYear(cy string) string {
	log.Printf("have classyear: %v", cy)
	if reGraduationYear.MatchString(cy) {
		return cy
	} else {
		switch strings.ToLower(cy) {
		case "freshman", "frosh", "fresh", "fr":
			return strconv.Itoa(time.Now().Year() + 4)
		case "sophomore", "soph", "so":
			return strconv.Itoa(time.Now().Year() + 3)
		case "junior", "jr":
			return strconv.Itoa(time.Now().Year() + 2)
		case "senior", "sr":
			return strconv.Itoa(time.Now().Year() + 1)
		default:
			return strconv.Itoa(time.Now().Year() + 4)
		}
	}
}

func CalcClassDesig(cy string) string {
	switch strings.ToLower(cy) {
	case "freshman", "frosh", "fresh", "fr":
		return "FR"
	case "sophomore", "soph", "so":
		return "SO"
	case "junior", "jr":
		return "JR"
	case "senior", "sr":
		return "SR"
	default:
		return ""
	}
}

func ParseName(v string) NameParsed {
	result := reFullName.FindStringSubmatch(v)
	if len(result) >= 3 {
		// ignore 0
		fname := result[1]
		lname := result[2]
		suffix := result[3]

		if strings.HasSuffix(fname, ",") {
			fname = result[2]
			lname = strings.TrimSuffix(result[1], ",")
		}
		return NameParsed{
			FNAME:  fname,
			LNAME:  lname,
			SUFFIX: suffix,
		}
	}
	return NameParsed{}
}

func GetOutputByType(s *[]PostRecord, t string) (*PostRecord, int) {
	for index, v := range *s {
		if v.Type == t {
			return &v, index
		}
	}
	v := PostRecord{
		Type:     t,
		Sequence: 1,
		Output:   PeopleOutput{},
	}
	*s = append(*s, v)
	return &v, len(*s) - 1
}

func GetOutputByTypeAndSequence(s *[]PostRecord, t string, i int) (*PostRecord, int) {
	for index, v := range *s {
		if v.Type == t && v.Sequence == i {
			return &v, index
		}
	}
	o := PeopleOutput{}
	if t == "mpr" {
		o.ROLE = MatchKeyField{
			Value:  "Parent",
			Source: "WM",
		}
	}
	v := PostRecord{
		Type:     t,
		Sequence: i,
		Output:   o,
	}
	*s = append(*s, v)
	return &v, len(*s) - 1
}

func LogDev(s string) {
	if dev {
		log.Printf(s)
	}
}
