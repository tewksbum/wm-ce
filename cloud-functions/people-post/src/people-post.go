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
	//trigger
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
	SALUTATION   MatchKeyField `json:"salutation" bigquery:"salutation"`
	NICKNAME     MatchKeyField `json:"nickname" bigquery:"nickname"`
	FNAME        MatchKeyField `json:"fname" bigquery:"fname"`
	FINITIAL     MatchKeyField `json:"finitial" bigquery:"finitial"`
	LNAME        MatchKeyField `json:"lname" bigquery:"lname"`

	AD1          MatchKeyField `json:"ad1" bigquery:"ad1"`
	AD2          MatchKeyField `json:"ad2" bigquery:"ad2"`
	AD3          MatchKeyField `json:"ad3" bigquery:"ad3"`
	CITY         MatchKeyField `json:"city" bigquery:"city"`
	STATE        MatchKeyField `json:"state" bigquery:"state"`
	ZIP          MatchKeyField `json:"zip" bigquery:"zip"`
	ZIP5         MatchKeyField `json:"zip5" bigquery:"zip5"`
	COUNTRY      MatchKeyField `json:"country" bigquery:"country"`
	MAILROUTE    MatchKeyField `json:"mailRoute" bigquery:"mailroute"`
	ADTYPE       MatchKeyField `json:"adType" bigquery:"adtype"`

	EMAIL        MatchKeyField `json:"email" bigquery:"email"`
	PHONE        MatchKeyField `json:"phone" bigquery:"phone"`
	
	TRUSTEDID    MatchKeyField `json:"trustedId" bigquery:"trustedid"`
	CLIENTID     MatchKeyField `json:"clientId" bigquery:"clientid"`

	GENDER       MatchKeyField `json:"gender" bigquery:"gender"`
	AGE          MatchKeyField `json:"age" bigquery:"age"`
	DOB          MatchKeyField `json:"dob" bigquery:"dob"`

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

type CityStateZip struct {
	Cities []string `json:"cities"`
	State  string   `json:"state"`
	Zip    string   `json:"zip"`
}

var ProjectID = os.Getenv("PROJECTID")
var PubSubTopic = os.Getenv("PSOUTPUT")
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

// TODO:  Role
// TODO:  Gender

func PostProcessPeople(ctx context.Context, m PubSubMessage) error {
	var input Input
	if err := json.Unmarshal(m.Data, &input); err != nil {
		log.Fatalf("Unable to unmarshal message %v with error %v", string(m.Data), err)
	}

	// assign match keys
	var mkOutput PeopleOutput
	var trustedID string
	var ClassYear string
	// var dormERR bool
	// var dormVER bool
	// var dormAD1 string
	// var dormColumn string
	// var roomColumn string
	var concatAdd bool
	var concatAddCol int
	var concatCityState bool
	var fullName bool
	var fullNameCol int
	var emailCount int
	var phoneCount int
	var emailList []int
	var phoneList []int
	var haveDorm bool // this should be abstracted...
	var dormCol int // this should be abstracted...
	var roomCol int // this should be abstracted...
	var mpr [3]PeopleOutput
	var memNumb int
	var addressInput string

	// MPR checks
	memNumb = 0
	
	// MAR checks
	emailCount = 0 
	phoneCount = 0
	haveDorm = false
	dormCol = 0
	roomCol = 0
	fullNameCol = 0

	log.Printf("people-post for record: %v", input.Signature.RecordID)

	for index, column := range input.Columns {
		
		// assign ML prediction to column
			predictionValue := input.Prediction.Predictions[index]
			predictionKey := strconv.Itoa(int(predictionValue))
			matchKey := MLLabels[predictionKey]
			column.MatchKey = matchKey
			// log.Printf("column %v index %v prediction value %v formatted %v label %v", column, index, predictionValue, predictionKey, matchKey)
		
		// ***** correct known wrong flags
			fullName = false
			concatAdd = false
			concatCityState = false
			// corrects the situation where FR, SO, JR, SR is identified as a country
			if column.PeopleERR.Title == 1 && matchKey == "COUNTRY" {
				matchKey = ""
				column.MatchKey = ""
				column.PeopleERR.Country = 0
			}
			nameParts := strings.Split(column.Value, " ") 
			if len(nameParts) > 1 && column.PeopleERR.FirstName == 1 && column.PeopleERR.LastName == 1 && column.PeopleERR.Role == 0 {
				fullName = true
				fullNameCol = index
			}
			if column.PeopleERR.Address1 == 1 && column.PeopleERR.State == 1 && column.PeopleERR.Role == 0 {
				concatAdd = true
				concatAddCol = index
			}
			if column.PeopleERR.City == 1 && column.PeopleERR.State == 1 && column.PeopleERR.Role == 0 {
				concatCityState = true
			}
//>>>>> TODO: Dorm detection needs help
			if column.PeopleERR.Dorm == 1 && reResidenceHall.MatchString(column.Value) {
				haveDorm = true
				dormCol = index
				log.Printf("dorm flagging true with: %v %v", column.Name, column.Value)
			}
			if column.PeopleERR.Room == 1 {				
				roomCol = index
			}

		// ***** source to matchkey mappings
			if column.PeopleERR.Title == 1 {
				SetMkField(&mkOutput, "TITLE", column.Value, column.Name)
			}
			if column.PeopleERR.TrustedID == 1 {
				trustedID = column.Value
				SetMkField(&mkOutput, "CLIENTID", trustedID, column.Name)
			}
			if column.PeopleERR.Organization == 1 {
				SetMkField(&mkOutput, "ORGANIZATION", column.Value, column.Name)
			}
			if matchKey == "" && column.PeopleERR.Title == 1 && len(column.Value) > 0 {
				SetMkField(&mkOutput, "TITLE", ClassYear, column.Name)
			}	
			//wtf does this do????  
			// if matchKey != "" {
			// 	// if it does not already have a value
			// 	if len(GetMkField(&mkOutput, matchKey).Value) == 0 {
			// 		SetMkField(&mkOutput, matchKey, column.Value, column.Name)
			// 	}
			// }

		// ***** correct values
			// fix zip code that has leading 0 stripped out
			if matchKey == "ZIP" && IsInt(column.Value) && len(column.Value) < 5 {
				column.Value = LeftPad2Len(column.Value, "0", 5)
			}

			// full name
			// below...

			// concat address
			// handling concatenated address ERR = Address, VAL = 1 Main Pleastenville
			// this is handled below w/ address parser
			
			// class year
			log.Printf("evaluating classyear %v %v %v", matchKey, column.PeopleERR.Title, column.Value )
			if matchKey == "" && column.PeopleERR.Title == 1 && len(column.Value) > 0 {
				log.Printf("have classyear: %v", column.Value)
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
				mkOutput.TITLE.Value = ClassYear
				mkOutput.TITLE.Source = column.Name
			}
		
			// AdType
			mkOutput.ADTYPE.Value = AssignAddressType(&column)

		// ***** construct student
			// start by taking column values at their name...
			// make a point to avoid mpr values
			// special cases = full name, concatenated address, mpr
 			if column.PeopleERR.LastName == 1 && column.PeopleERR.FirstName == 0 && column.PeopleERR.Address1 == 0 && column.PeopleERR.City == 0 && column.PeopleERR.Role == 0 && !fullName {
				log.Printf("have LNAME: %v", column.Value)
				mkOutput.LNAME.Value = column.Value
				mkOutput.LNAME.Source = column.Name
			} else if column.PeopleERR.FirstName == 1 && column.PeopleERR.LastName == 0 && column.PeopleERR.Address1 == 0 && column.PeopleERR.City == 0 && column.PeopleERR.Role == 0 && !fullName {
				mkOutput.FNAME.Value = column.Value
				mkOutput.FNAME.Source = column.Name
			} else if column.PeopleERR.Address1 == 1 && column.PeopleERR.Address2 == 0 && column.PeopleERR.Address3 == 0 && column.PeopleERR.FirstName == 0 && column.PeopleERR.LastName == 0 && column.PeopleERR.Role == 0 && !column.PeopleVER.IS_EMAIL && !concatAdd && !concatCityState {
				mkOutput.AD1.Value = column.Value
				mkOutput.AD1.Source = column.Name
			} else if column.PeopleERR.Address2 == 1 && column.PeopleERR.FirstName == 0 && column.PeopleERR.LastName == 0 && column.PeopleERR.Role == 0 && !concatAdd && !concatCityState {
				mkOutput.AD2.Value = column.Value
				mkOutput.AD2.Source = column.Name	
			} else if column.PeopleERR.Address3 == 1 && column.PeopleERR.FirstName == 0 && column.PeopleERR.LastName == 0 && column.PeopleERR.Role == 0 && !concatAdd && !concatCityState {
				mkOutput.AD3.Value = column.Value
				mkOutput.AD3.Source = column.Name	
			} else if column.PeopleERR.City == 1 && column.PeopleERR.FirstName == 0 && column.PeopleERR.LastName == 0 && column.PeopleERR.Role == 0 && !concatAdd && !concatCityState {
				mkOutput.CITY.Value = column.Value
				mkOutput.CITY.Source = column.Name
			} else if column.PeopleERR.State == 1 && column.PeopleERR.Role == 0 && !concatAdd && !concatCityState {
				mkOutput.STATE.Value = column.Value
				mkOutput.STATE.Source = column.Name
			} else if column.PeopleERR.ZipCode == 1 && column.PeopleERR.Role == 0 && !concatAdd && !concatCityState {
				mkOutput.ZIP.Value = column.Value
				mkOutput.ZIP.Source = column.Name
			} else if column.PeopleERR.Country == 1 && column.PeopleERR.Role == 0 {
				log.Printf("Setting Country: %v %v", column.PeopleERR.Country, column.Value)
				mkOutput.COUNTRY.Value = column.Value
				mkOutput.COUNTRY.Source = column.Name
			}
			
			// override ERR w/ VER...
			// make a point to avoid mpr values
			if column.PeopleVER.IS_LASTNAME && column.PeopleERR.Address1 == 0 && column.PeopleERR.City == 0 && column.PeopleERR.Role == 0 && !fullName {
				log.Printf("have LNAME: %v", column.Value)
				mkOutput.LNAME.Value = column.Value
				mkOutput.LNAME.Source = column.Name
			} else if column.PeopleVER.IS_FIRSTNAME && column.PeopleERR.Address1 == 0 && column.PeopleERR.City == 0 && column.PeopleERR.Role == 0 && !fullName {
				mkOutput.FNAME.Value = column.Value
				mkOutput.FNAME.Source = column.Name
			} else if column.PeopleVER.IS_STREET1 && column.PeopleERR.FirstName == 0 && column.PeopleERR.LastName == 0 && column.PeopleERR.Role == 0 && !column.PeopleVER.IS_EMAIL && !concatAdd && !concatCityState {
				mkOutput.AD1.Value = column.Value
				mkOutput.AD1.Source = column.Name
			}  else if column.PeopleVER.IS_CITY && column.PeopleERR.FirstName == 0 && column.PeopleERR.LastName == 0 && column.PeopleERR.Role == 0 && !concatAdd && !concatCityState {
				// Beverly... ERR City = 1, Fname = 0, Lname = 0 / VER Fname = 1, Lname = 1, City = 1
				mkOutput.CITY.Value = column.Value
				mkOutput.CITY.Source = column.Name
			} else if column.PeopleVER.IS_STATE && column.PeopleERR.Role == 0 && !concatAdd && !concatCityState {
				mkOutput.STATE.Value = column.Value
				mkOutput.STATE.Source = column.Name
			} else if column.PeopleVER.IS_ZIPCODE && column.PeopleERR.Role == 0 && !concatAdd && !concatCityState {
				mkOutput.ZIP.Value = column.Value
				mkOutput.ZIP.Source = column.Name
			} else if column.PeopleVER.IS_COUNTRY && column.PeopleERR.Title == 0 && column.PeopleERR.Role == 0 {
				log.Printf("Setting VER Country: %v %v", column.PeopleVER.IS_COUNTRY, column.Value)
				mkOutput.COUNTRY.Value = column.Value
				mkOutput.COUNTRY.Source = column.Name
			}

			// if column.PeopleVER.IS_FIRSTNAME && column.PeopleERR.FirstName == 1 && column.PeopleERR.Address1 == 0 && column.PeopleERR.City == 0 && column.PeopleERR.Role == 0 && !fullName {	
			if column.PeopleVER.IS_FIRSTNAME && column.PeopleERR.FirstName == 1 && column.PeopleERR.Role == 0 && !fullName {	
				mkOutput.FNAME.Value = column.Value
				mkOutput.FNAME.Source = column.Name
			} 
			// if column.PeopleVER.IS_LASTNAME && column.PeopleERR.LastName == 1 && column.PeopleERR.Address1 == 0 && column.PeopleERR.City == 0 && column.PeopleERR.Role == 0 && !fullName {	
			if column.PeopleVER.IS_LASTNAME && column.PeopleERR.LastName == 1 && column.PeopleERR.Role == 0 && !fullName {
				mkOutput.FNAME.Value = column.Value
				mkOutput.FNAME.Source = column.Name
			} 

			if column.PeopleVER.IS_FIRSTNAME && column.PeopleVER.IS_LASTNAME && column.PeopleERR.Address1 == 0 && column.PeopleERR.City == 0 && column.PeopleERR.Role == 0 && !fullName {
				if column.PeopleERR.FirstName == 1 {
					mkOutput.FNAME.Value = column.Value
					mkOutput.FNAME.Source = column.Name
				} else if column.PeopleERR.LastName == 1 {
					mkOutput.LNAME.Value = column.Value
					mkOutput.LNAME.Source = column.Name
				}
			} 
			
			// phone & email ONLY check VER
			// make a point to avoid mpr values
			if column.PeopleVER.IS_EMAIL && column.PeopleERR.Role == 0 {
				mkOutput.EMAIL.Value = column.Value
				mkOutput.EMAIL.Source = column.Name
				// type email if ends with gmail, yahoo, hotmail
				if len(mkOutput.EMAIL.Value) > 0 {
					email := strings.ToLower(mkOutput.EMAIL.Value)
					if strings.HasSuffix(email, "gmail.com") || strings.HasSuffix(email, "yahoo.com") || strings.HasSuffix(email, "hotmail.com") {
						mkOutput.EMAIL.Type = "Private"
					}
				}
				emailCount = emailCount + 1
				emailList = append(emailList, index) 
			} else if column.PeopleVER.IS_PHONE && column.PeopleERR.Role == 0 && len(column.Value) >= 10 {
				numberValue := reNumberOnly.ReplaceAllString(column.Value, "")
				if len(numberValue) == 10 || (len(numberValue) == 11 && strings.HasPrefix(numberValue, "1")) {
					mkOutput.PHONE.Value = column.Value
					mkOutput.PHONE.Source = column.Name
				}
				phoneCount = phoneCount + 1
				phoneList = append(phoneList, index) 
			}

		// ***** construct MPR
		memNumb = extractMemberNumb(column.Name)
		log.Printf("memNumb: %v", memNumb)
		if column.PeopleERR.LastName == 1 && column.PeopleERR.Address1 == 0 && column.PeopleERR.City == 0 && column.PeopleERR.Role == 1 && !fullName {
			log.Printf("trying to assign mpr lastname: %v %v", column.Value, memNumb)
			mpr[memNumb].LNAME.Value = column.Value
			mpr[memNumb].LNAME.Source = column.Name
		} else if column.PeopleERR.FirstName == 1 && column.PeopleERR.LastName == 0 && column.PeopleERR.Address1 == 0 && column.PeopleERR.City == 0 && column.PeopleERR.Role == 1 && !fullName {
			log.Printf("trying to assign mpr firstname: %v %v", column.Value, memNumb)
			mpr[memNumb].FNAME.Value = column.Value
			mpr[memNumb].FNAME.Source = column.Name
		}  else if column.PeopleERR.Address1 == 1 && column.PeopleERR.FirstName == 0 && column.PeopleERR.LastName == 0 && column.PeopleERR.Role == 1 && !column.PeopleVER.IS_EMAIL && !concatAdd && !concatCityState {
			log.Printf("trying to assign mpr ad1: %v %v", column.Value, memNumb)
			mpr[memNumb].AD1.Value = column.Value
			mpr[memNumb].AD1.Source = column.Name
		} else if column.PeopleERR.Address2 == 1 && column.PeopleERR.FirstName == 0 && column.PeopleERR.LastName == 0 && column.PeopleERR.Role == 1 && !concatAdd && !concatCityState {
			mpr[memNumb].AD2.Value = column.Value
			mpr[memNumb].AD2.Source = column.Name
		} else if column.PeopleERR.Address3 == 1 && column.PeopleERR.FirstName == 0 && column.PeopleERR.LastName == 0 && column.PeopleERR.Role == 1 && !concatAdd && !concatCityState {
			mpr[memNumb].AD3.Value = column.Value
			mpr[memNumb].AD3.Source = column.Name	
		} else if column.PeopleERR.City == 1 && column.PeopleERR.FirstName == 0 && column.PeopleERR.LastName == 0 && column.PeopleERR.Role == 1 && !column.PeopleVER.IS_STATE && !concatAdd && !concatCityState {
			mpr[memNumb].CITY.Value = column.Value
			mpr[memNumb].CITY.Source = column.Name
		} else if column.PeopleERR.State == 1 && column.PeopleERR.Role == 1 && !concatAdd && !concatCityState {
			mpr[memNumb].STATE.Value = column.Value
			mpr[memNumb].STATE.Source = column.Name
		} else if column.PeopleERR.ZipCode == 1 && column.PeopleERR.Role == 1 && !concatAdd && !concatCityState {
			mpr[memNumb].ZIP.Value = column.Value
			mpr[memNumb].ZIP.Source = column.Name
		} else if column.PeopleERR.Country == 1 && column.PeopleERR.Title == 0 && column.PeopleERR.Role == 1 {
			mpr[memNumb].COUNTRY.Value = column.Value
			mpr[memNumb].COUNTRY.Source = column.Name
		}
		
		if column.PeopleVER.IS_LASTNAME && column.PeopleERR.Address1 == 0 && column.PeopleERR.City == 0 && column.PeopleERR.Role == 1 && !fullName {
			mpr[memNumb].LNAME.Value = column.Value
			mpr[memNumb].LNAME.Source = column.Name
		} else if column.PeopleVER.IS_FIRSTNAME && column.PeopleERR.Address1 == 0 && column.PeopleERR.City == 0 && column.PeopleERR.Role == 1 && !fullName {
			mpr[memNumb].FNAME.Value = column.Value
			mpr[memNumb].FNAME.Source = column.Name
		} else if column.PeopleVER.IS_STREET1 && column.PeopleERR.FirstName == 0 && column.PeopleERR.Role == 1 && column.PeopleERR.LastName == 0 && !column.PeopleVER.IS_EMAIL && !concatAdd && !concatCityState {
			mpr[memNumb].AD1.Value = column.Value
			mpr[memNumb].AD1.Source = column.Name
		}  else if column.PeopleVER.IS_CITY && column.PeopleERR.FirstName == 0 && column.PeopleERR.Role == 1 && column.PeopleERR.LastName == 0 && !column.PeopleVER.IS_STATE && !concatAdd && !concatCityState {
			// Beverly... ERR City = 1, Fname = 0, Lname = 0 / VER Fname = 1, Lname = 1, City = 1
			mpr[memNumb].CITY.Value = column.Value
			mpr[memNumb].CITY.Source = column.Name
		} else if column.PeopleVER.IS_STATE && column.PeopleERR.Role == 1 && !concatAdd && !concatCityState {
			mpr[memNumb].STATE.Value = column.Value
			mpr[memNumb].STATE.Source = column.Name
		} else if column.PeopleVER.IS_ZIPCODE && column.PeopleERR.Role == 1 && !concatAdd && !concatCityState {
			mpr[memNumb].ZIP.Value = column.Value
			mpr[memNumb].ZIP.Source = column.Name
		} else if column.PeopleVER.IS_COUNTRY && column.PeopleERR.Title == 0 && column.PeopleERR.Role == 1 {
			mpr[memNumb].COUNTRY.Value = column.Value
			mpr[memNumb].COUNTRY.Source = column.Name
		}

		if column.PeopleVER.IS_FIRSTNAME && column.PeopleVER.IS_LASTNAME && column.PeopleERR.Address1 == 0 && column.PeopleERR.City == 0 && column.PeopleERR.Role == 1 && !fullName {
			if column.PeopleERR.FirstName == 1 {
				mpr[memNumb].FNAME.Value = column.Value
				mpr[memNumb].FNAME.Source = column.Name
			} else if column.PeopleERR.LastName == 1 {
				mpr[memNumb].LNAME.Value = column.Value
				mpr[memNumb].LNAME.Source = column.Name
			}
		} 
		
		if column.PeopleVER.IS_EMAIL && column.PeopleERR.Role == 1 {
			log.Printf("trying to assign mpr ad1: %v %v", column.Value, memNumb)
			mpr[memNumb].EMAIL.Value = column.Value
			mpr[memNumb].EMAIL.Source = column.Name
			// type email if ends with gmail, yahoo, hotmail
			if len(mkOutput.EMAIL.Value) > 0 {
				email := strings.ToLower(mkOutput.EMAIL.Value)
				if strings.HasSuffix(email, "gmail.com") || strings.HasSuffix(email, "yahoo.com") || strings.HasSuffix(email, "hotmail.com") {
					mpr[memNumb].EMAIL.Type = "Private"
				}
			}
		} else if column.PeopleVER.IS_PHONE && column.PeopleERR.Role == 1 && len(column.Value) >= 10 {
			numberValue := reNumberOnly.ReplaceAllString(column.Value, "")
			if len(numberValue) == 10 || (len(numberValue) == 11 && strings.HasPrefix(numberValue, "1")) {
				mpr[memNumb].PHONE.Value = column.Value
				mpr[memNumb].PHONE.Source = column.Name
			}
		}

		input.Columns[index] = column
	}

//>>>>> TODO: fullname needs help
	if fullName {
		log.Printf("fullname w/ name parts: %v", fullNameCol)

		// know the column w/ a full name... fullNameCol
		
		// namePartz := strings.Split(input.Columns[fullNameCol].Value, " ") 
		// for i := 1; i < len(namePartz); i++ {
			// run them against VER for first & last name
			// store the values to output
			// mkOutput.FNAME.Value = column.Value
			// mkOutput.LNAME.Value = column.Value
			// IS_FIRSTNAME: ContainsBool(listFirstNames, val),
			// IS_LASTNAME:  ContainsBool(listLastNames, val),
		// }

		mkOutput.FNAME.Source = input.Columns[fullNameCol].Name
		mkOutput.LNAME.Source = input.Columns[fullNameCol].Name
	}

	// parse address as needed
	if(!concatAdd) {
		addressInput = mkOutput.AD1.Value + " " + mkOutput.AD2.Value + " " + mkOutput.CITY.Value + " " + mkOutput.STATE.Value + " " + mkOutput.ZIP.Value
	} else {
		addressInput = input.Columns[concatAddCol].Value
	}
	a := ParseAddress(addressInput)
	log.Printf("address parser returned %v", a)
	if len(a.City) > 0 {
		mkOutput.AD1.Value = a.Number + " " + a.Street + " " + a.Type
		mkOutput.AD2.Value = a.SecUnitType + " " + a.SecUnitNum
		mkOutput.AD3.Value = ""
		mkOutput.CITY.Value = a.City
		mkOutput.STATE.Value = a.State
		mkOutput.ZIP.Value = a.Zip
		if len(a.Plus4) > 0 {
			mkOutput.ZIP.Value = a.Zip + "-" + a.Plus4
		}
	}

	// check zip city state match
	ZipCheck := CheckCityStateZip(mkOutput.CITY.Value, mkOutput.STATE.Value, mkOutput.ZIP.Value)
// >>>>>disable during development...
	if ZipCheck == false && len(mkOutput.AD1.Value) > 0 && false {
		address := strings.Join([]string{mkOutput.AD1.Value, mkOutput.AD2.Value, mkOutput.CITY.Value, mkOutput.STATE.Value, mkOutput.ZIP.Value}, ",")
		correctedOutputAddress := CorrectAddress(address)
		if len(correctedOutputAddress) > 0 {
			mkOutput.AD1.Value = strings.Join([]string{correctedOutputAddress[0].Components.PrimaryNumber, " ", correctedOutputAddress[0].Components.StreetPredirection, " ", correctedOutputAddress[0].Components.StreetName, " ", correctedOutputAddress[0].Components.StreetSuffix}, "")
			mkOutput.AD2.Value = strings.Join([]string{correctedOutputAddress[0].Components.SecondaryDesignator, " ", correctedOutputAddress[0].Components.SecondaryNumber}, "")			
			mkOutput.CITY.Value = correctedOutputAddress[0].Components.CityName
			mkOutput.STATE.Value = correctedOutputAddress[0].Components.StateAbbreviation
			mkOutput.ZIP.Value = correctedOutputAddress[0].Components.Zipcode
		}
	}

	// pub the record
	log.Printf("pubbing student...")
	pubRecord(ctx, &input, mkOutput)

	// handle MAR values
	if emailCount > 1 {
		log.Printf("Have multiple emails: %v", emailCount)
		for i := 1; i < len(emailList); i++ {
			// update email value... and resend...
			mkOutput.EMAIL.Value = input.Columns[emailList[i]].Value
			mkOutput.EMAIL.Source = input.Columns[emailList[i]].Name
			if len(mkOutput.EMAIL.Value) > 0 {
				email := strings.ToLower(mkOutput.EMAIL.Value)
				if strings.HasSuffix(email, "gmail.com") || strings.HasSuffix(email, "yahoo.com") || strings.HasSuffix(email, "hotmail.com") {
					mkOutput.EMAIL.Type = "Private"
				}
			}
			log.Printf("pubbing MAR email %v ", mkOutput.EMAIL.Value)
			pubRecord(ctx, &input, mkOutput)
		}
	}
	if phoneCount > 1 {
		log.Printf("Have multiple phones: %v", phoneCount)
		for i := 1; i < len(phoneList); i++ {
			// update phone value... and resend...
			mkOutput.PHONE.Value = input.Columns[phoneList[i]].Value
			mkOutput.PHONE.Source = input.Columns[phoneList[i]].Name
			log.Printf("pubbing MAR phone %v ", mkOutput.PHONE.Value)
			pubRecord(ctx, &input, mkOutput)
		}
	}
	if haveDorm {
		mkOutput.AD1.Value = input.Columns[dormCol].Value
		mkOutput.AD1.Source = input.Columns[dormCol].Name
		if roomCol > 0 {
			mkOutput.AD2.Value = input.Columns[roomCol].Value
			mkOutput.AD2.Source = input.Columns[roomCol].Name
		} else {
			mkOutput.AD2.Value = ""
		}
		mkOutput.CITY.Value = ""
		mkOutput.STATE.Value = ""
		mkOutput.ZIP.Value = ""
		mkOutput.ADTYPE.Value = "Campus"
		log.Printf("pubbing Dorm %v ", input.Columns[dormCol].Name)
		pubRecord(ctx, &input, mkOutput)
	}

	// handle mpr
	for i := 0; i < len(mpr); i++ {
		log.Printf("mpr loop %v", i)
		log.Printf("checking if have mpr value %v %v", mpr[i].FNAME.Value, mpr[i].EMAIL.Value)
		if (mpr[i].FNAME.Value != "") || (mpr[i].EMAIL.Value != "") {
			log.Printf("have mpr value %v", i)
			if mpr[i].FNAME.Value != "" {
				mkOutput.FNAME.Value = mpr[i].FNAME.Value
				mkOutput.FNAME.Source = mpr[i].FNAME.Source	
			} else {
				mkOutput.FNAME.Value = ""
				mkOutput.FNAME.Source = ""
			}
			if mpr[i].LNAME.Value != "" {
				mkOutput.LNAME.Value = mpr[i].LNAME.Value
				mkOutput.LNAME.Source = mpr[i].LNAME.Source	
			}
			if mpr[i].AD1.Value != "" {
				mkOutput.AD1.Value = mpr[i].AD1.Value
				mkOutput.AD1.Source = mpr[i].AD1.Source	
			}
			if mpr[i].AD2.Value != "" {
				mkOutput.AD2.Value = mpr[i].AD2.Value
				mkOutput.AD2.Source = mpr[i].AD2.Source	
			}
			if mpr[i].AD3.Value != "" {
				mkOutput.AD3.Value = mpr[i].AD3.Value
				mkOutput.AD3.Source = mpr[i].AD3.Source	
			}
			if mpr[i].CITY.Value != "" {
				mkOutput.CITY.Value = mpr[i].CITY.Value
				mkOutput.CITY.Source = mpr[i].CITY.Source	
			}
			if mpr[i].STATE.Value != "" {
				mkOutput.STATE.Value = mpr[i].STATE.Value
				mkOutput.STATE.Source = mpr[i].STATE.Source	
			}
			if mpr[i].ZIP.Value != "" {
				mkOutput.ZIP.Value = mpr[i].ZIP.Value
				mkOutput.ZIP.Source = mpr[i].ZIP.Source	
			}
			if mpr[i].EMAIL.Value != "" {
				mkOutput.EMAIL.Value = mpr[i].EMAIL.Value
				mkOutput.EMAIL.Source = mpr[i].EMAIL.Source	
			} else {
				mkOutput.EMAIL.Value = ""
				mkOutput.EMAIL.Source = ""
			}
			if mpr[i].PHONE.Value != "" {
				mkOutput.PHONE.Value = mpr[i].PHONE.Value
				mkOutput.PHONE.Source = mpr[i].PHONE.Source	
			} else {
				mkOutput.PHONE.Value = ""
				mkOutput.PHONE.Source = ""
			}

			addressInput := mkOutput.AD1.Value + " " + mkOutput.AD2.Value + " " + mkOutput.CITY.Value + " " + mkOutput.STATE.Value + " " + mkOutput.ZIP.Value
			a := ParseAddress(addressInput)
			log.Printf("address parser returned %v", a)
			if len(a.City) > 0 {
				mkOutput.AD1.Value = a.Number + " " + a.Street + " " + a.Type
				mkOutput.AD2.Value = a.SecUnitType + " " + a.SecUnitNum
				mkOutput.AD3.Value = ""
				mkOutput.CITY.Value = a.City
				mkOutput.STATE.Value = a.State
				mkOutput.ZIP.Value = a.Zip
				if len(a.Plus4) > 0 {
					mkOutput.ZIP.Value = a.Zip + "-" + a.Plus4
				}
			}

			mkOutput.ROLE.Value = "parent"  // this should be generalized
			pubRecord(ctx, &input, mkOutput)
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

// I'm guessing what this does is record SOR >< MatchKey field mapping... for ABM
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
	baseUrl, err := url.Parse(AddressParserBaseUrl)
	baseUrl.Path += AddressParserPath
	params := url.Values{}
	params.Add("a", address)
	baseUrl.RawQuery = params.Encode()

	req, err := http.NewRequest(http.MethodGet, baseUrl.String(), nil)
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
	} else {
		log.Printf("address parser reponse: %v", string(body))
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

func extractMemberNumb(colVal string) int {
	if strings.Contains(colVal, "first") || strings.Contains(colVal, "1") || strings.Contains(colVal, "father") {
		return 0
	}
	if strings.Contains(colVal, "second") || strings.Contains(colVal, "2") || strings.Contains(colVal, "mother") {
		return 1
	}
	if strings.Contains(colVal, "third") || strings.Contains(colVal, "3") {
		return 2
	}
	return 0
}

func pubRecord(ctx context.Context, input *Input, mkOutput PeopleOutput) {
	var output Output
	output.Signature = input.Signature
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