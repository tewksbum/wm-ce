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
	// blow
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
	MatchKey  string    `json:"MK"`
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

	AD1       MatchKeyField `json:"ad1" bigquery:"ad1"`
	AD1NO     MatchKeyField `json:"ad1no" bigquery:"ad1no"`
	AD2       MatchKeyField `json:"ad2" bigquery:"ad2"`
	AD3       MatchKeyField `json:"ad3" bigquery:"ad3"`
	CITY      MatchKeyField `json:"city" bigquery:"city"`
	STATE     MatchKeyField `json:"state" bigquery:"state"`
	ZIP       MatchKeyField `json:"zip" bigquery:"zip"`
	ZIP5      MatchKeyField `json:"zip5" bigquery:"zip5"`
	COUNTRY   MatchKeyField `json:"country" bigquery:"country"`
	MAILROUTE MatchKeyField `json:"mailroute" bigquery:"mailroute"`
	ADTYPE    MatchKeyField `json:"adtype" bigquery:"adtype"`
	ADPARSER  MatchKeyField `json:"adparser" bigquery:"adparser"`
	ADCORRECT MatchKeyField `json:"adcorrect" bigquery:"adcorrect"`

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
	ContainsName    	int `json:"ContainsName"`
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
var reState = regexp.MustCompile(`(?i)^AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY|DC$`)

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

	// assign match keys
	var mkOutput PeopleOutput
	var trustedID string
	// var ClassYear string
	var concatAdd bool
	var concatAddCol int
	var concatCityState bool
	var concatCityStateCol int
	var fullName bool
	var emailCount int
	var phoneCount int
	var emailList []int
	var phoneList []int
	var haveDorm bool // this should be abstracted...
	var dormCol int   // this should be abstracted...
	var roomCol int   // this should be abstracted...
	var mpr [3]PeopleOutput
	var memNumb int
	// var addressInput string
	var roleCount int

	// MPR checks
	memNumb = 0

	// MAR checks
	emailCount = 0
	phoneCount = 0
	haveDorm = false
	dormCol = 0
	roomCol = 0

	log.Printf("people-post for record: %v", input.Signature.RecordID)

	// iterate through every column on the input record to decide what the column is...
	for index, column := range input.Columns {

		fullName = false
		concatAdd = false
		// concatCityState = false
		memNumb = extractMemberNumb(column.Name) // used for mpr
		roleCount = 0

		// assign ML prediction to column
		predictionValue := input.Prediction.Predictions[index]
		predictionKey := strconv.Itoa(int(predictionValue))
		matchKey := MLLabels[predictionKey]
		column.MatchKey = matchKey

		// AdType
		mkOutput.ADTYPE.Value = AssignAddressType(&column)
		// mkOutput.ADCORRECT.Value = 0

		if dev {
			log.Printf("Posting column, value, prediction: %v %v %v %v", column.Name, column.Value, matchKey, input.Signature.EventID)
		}

		// ***** set high confidence items
		if column.PeopleERR.TrustedID == 1 {
			trustedID = column.Value
			SetMkField(&mkOutput, "CLIENTID", trustedID, column.Name)
		} else if column.PeopleERR.Organization == 1 {
			SetMkField(&mkOutput, "ORGANIZATION", column.Value, column.Name)
		} else if column.PeopleERR.Gender == 1 {
// TODO: can we do some automated detection here...
			SetMkField(&mkOutput, "GENDER", column.Value, column.Name)
		} else if column.PeopleERR.ContainsStudentRole == 1 {
			SetMkField(&mkOutput, "ROLE", column.Value, column.Name)
			roleCount++
		else if column.PeopleERR.Title == 1 || column.PeopleERR.ContainsTitle == 1 {
			// corrects the situation where FR, SO, JR, SR is identified as a country
			if dev {
				log.Printf("Title flagging true with: %v %v %v", column.Name, column.Value, input.Signature.EventID)
			}
			SetMkField(&mkOutput, "TITLE", calcClassYear(column.Value), column.Name)
			SetMkField(&mkOutput, "STATUS", calcClassDesig(column.Value), column.Name)
			matchKey = ""
			column.MatchKey = ""
			column.PeopleERR.Country = 0 // override this is NOT a country
			column.PeopleERR.State = 0   // override this is NOT a state value
		} else if column.PeopleERR.Dorm == 1 && reResidenceHall.MatchString(column.Value) {
			if dev {
				log.Printf("dorm flagging true with: %v %v %v", column.Name, column.Value, input.Signature.EventID)
			}
			haveDorm = true
			dormCol = index
		} else if column.PeopleERR.Room == 1 {
			roomCol = index
		} else if column.PeopleERR.FullAddress == 1 {
			if dev {
				log.Printf("Full Address: %v %v %v", column.Name, column.Value, input.Signature.EventID)
			}
			concatAdd = true
			concatAddCol = index
		} else if column.PeopleERR.ContainsState == 1 && column.PeopleERR.ContainsCity == 1 {
			if dev {
				log.Printf("ConcatAddress: %v %v %v", column.Name, column.Value, input.Signature.EventID)
			}
			concatCityState = true
			concatCityStateCol = index
		}

		if column.PeopleERR.ContainsRole == 0 {
			if dev {
				log.Printf("People role: %v", input.Signature.EventID)
			}
			// ***** check primary first
			// if we detect a fullname, stop checking everything else
			fullName = checkSetFullName(&mkOutput, column)
			// could do something here to make it immutable... if we get a hit... then suppress all other name values?
			if fullName {
				if dev {
					log.Printf("tagged as fullname %v", input.Signature.EventID)
				}
				// do we REALLY want to be doing this?
				// this will only apply to the current pass...
				column.MatchKey = ""
				column.PeopleERR.FirstName = 0
				column.PeopleVER.IS_FIRSTNAME = false
				column.PeopleERR.LastName = 0
				column.PeopleVER.IS_LASTNAME = false
				column.PeopleERR.ContainsFirstName = 0
				column.PeopleERR.ContainsLastName = 0
			} else if column.PeopleVER.IS_FIRSTNAME && column.PeopleERR.FirstName == 1 {
				if dev {
					log.Printf("FName with VER & ERR: %v %v %v", column.Name, column.Value, input.Signature.EventID)
				}
				SetMkField(&mkOutput, "FNAME", column.Value, column.Name)
			} else if column.PeopleVER.IS_LASTNAME && column.PeopleERR.LastName == 1 {
				if dev {
					log.Printf("LName with VER & ERR &: %v %v %v", column.Name, column.Value, input.Signature.EventID)
				}
				SetMkField(&mkOutput, "LNAME", column.Value, column.Name)
			} else if column.PeopleVER.IS_STREET1 && column.PeopleERR.Address1 == 1 {
				if dev {
					log.Printf("Address 1 VER & ERR: %v %v %v", column.Name, column.Value, input.Signature.EventID)
				}
				SetMkField(&mkOutput, "AD1", column.Value, column.Name)
			} else if column.PeopleVER.IS_STREET2 && column.PeopleERR.Address2 == 1 {
				if dev {
					log.Printf("Address 2 VER & ERR: %v %v %v", column.Name, column.Value, input.Signature.EventID)
				}
				SetMkField(&mkOutput, "AD2", column.Value, column.Name)
			} else if column.PeopleVER.IS_STREET3 && column.PeopleERR.Address3 == 1 {
				if dev {
					log.Printf("Address 3 VER & ERR: %v %v %v", column.Name, column.Value, input.Signature.EventID)
				}
				SetMkField(&mkOutput, "AD3", column.Value, column.Name)
			} else if column.PeopleVER.IS_CITY && column.PeopleERR.City == 1 {
				if dev {
					log.Printf("CITY VER & ERR: %v %v %v", column.Name, column.Value, input.Signature.EventID)
				}
				SetMkField(&mkOutput, "CITY", column.Value, column.Name)
			} else if column.PeopleVER.IS_STATE && column.PeopleERR.State == 1 {
				if dev {
					log.Printf("STATE VER & ERR: %v %v %v", column.Name, column.Value, input.Signature.EventID)
				}
				SetMkField(&mkOutput, "STATE", column.Value, column.Name)
			} else if column.PeopleVER.IS_ZIPCODE && column.PeopleERR.ZipCode == 1 {
				if dev {
					log.Printf("ZIP VER & ERR: %v %v %v", column.Name, column.Value, input.Signature.EventID)
				}
				SetMkField(&mkOutput, "ZIP", column.Value, column.Name)
				// fix zip code that has leading 0 stripped out
				if matchKey == "ZIP" && IsInt(column.Value) && len(column.Value) < 5 {
					column.Value = LeftPad2Len(column.Value, "0", 5)
				}
			} else if column.PeopleVER.IS_COUNTRY && column.PeopleERR.Country == 1 {
				if dev {
					log.Printf("Country VER & ERR: %v %v %v", column.Name, column.Value, input.Signature.EventID)
				}
				SetMkField(&mkOutput, "COUNTRY", column.Value, column.Name)
			} else if column.PeopleVER.IS_EMAIL {
				// phone & email ONLY check VER
				if dev {
					log.Printf("Email VER: %v %v %v", column.Name, column.Value, input.Signature.EventID)
				}
				SetMkField(&mkOutput, "EMAIL", column.Value, column.Name)
				// type email if ends with gmail, yahoo, hotmail
				if len(mkOutput.EMAIL.Value) > 0 {
					email := strings.ToLower(mkOutput.EMAIL.Value)
					if strings.HasSuffix(email, "gmail.com") || strings.HasSuffix(email, "yahoo.com") || strings.HasSuffix(email, "hotmail.com") {
						mkOutput.EMAIL.Type = "Private"
					}
				}
				emailCount = emailCount + 1
				emailList = append(emailList, index)
			} else if column.PeopleVER.IS_PHONE && len(column.Value) >= 10 {
				numberValue := reNumberOnly.ReplaceAllString(column.Value, "")
				if len(numberValue) == 10 || (len(numberValue) == 11 && strings.HasPrefix(numberValue, "1")) {
					if dev {
						log.Printf("Phone VER & US format: %v %v %v", column.Name, column.Value, input.Signature.EventID)
					}
					SetMkField(&mkOutput, "PHONE", column.Value, column.Name)
				}
				phoneCount = phoneCount + 1
				phoneList = append(phoneList, index)
			} else if column.PeopleERR.ContainsFirstName == 1 && column.PeopleVER.IS_FIRSTNAME {
				if dev {
					log.Printf("FName with VER + loose ERR: %v %v %v", column.Name, column.Value, input.Signature.EventID)
				}
				SetMkField(&mkOutput, "FNAME", column.Value, column.Name)
			} else if column.PeopleERR.FirstName == 1 {
				if dev {
					log.Printf("FName with ERR: %v %v %v", column.Name, column.Value, input.Signature.EventID)
				}
				SetMkField(&mkOutput, "FNAME", column.Value, column.Name)
			} else if column.PeopleERR.ContainsLastName == 1 && column.PeopleVER.IS_LASTNAME {
				if dev {
					log.Printf("LName with VER + loose ERR: %v %v %v", column.Name, column.Value, input.Signature.EventID)
				}
				SetMkField(&mkOutput, "LNAME", column.Value, column.Name)
			} else if column.PeopleERR.LastName == 1 {
				if dev {
					log.Printf("LName with ERR: %v %v %v", column.Name, column.Value, input.Signature.EventID)
				}
				SetMkField(&mkOutput, "LNAME", column.Value, column.Name)
			} else if column.PeopleERR.Address1 == 1 {
				if dev {
					log.Printf("ERR Address 1: %v %v %v", column.Name, column.Value, input.Signature.EventID)
				}
				SetMkField(&mkOutput, "AD1", column.Value, column.Name)
			} else if column.PeopleERR.Address2 == 1 {
				if dev {
					log.Printf("ERR Address 2: %v %v %v", column.Name, column.Value, input.Signature.EventID)
				}
				SetMkField(&mkOutput, "AD2", column.Value, column.Name)
			} else if column.PeopleERR.Address3 == 1 {
				if dev {
					log.Printf("ERR Address 3: %v %v %v", column.Name, column.Value, input.Signature.EventID)
				}
				SetMkField(&mkOutput, "AD3", column.Value, column.Name)
			} else if column.PeopleERR.City == 1 {
				if dev {
					log.Printf("ERR CITY: %v %v %v", column.Name, column.Value, input.Signature.EventID)
				}
				SetMkField(&mkOutput, "CITY", column.Value, column.Name)
			} else if column.PeopleERR.State == 1 {
				if dev {
					log.Printf("ERR STATE: %v %v %v", column.Name, column.Value, input.Signature.EventID)
				}
				SetMkField(&mkOutput, "STATE", column.Value, column.Name)
			} else if column.PeopleERR.ZipCode == 1 {
				if dev {
					log.Printf("ERR ZIP: %v %v %v", column.Name, column.Value, input.Signature.EventID)
				}
				SetMkField(&mkOutput, "ZIP", column.Value, column.Name)
				// fix zip code that has leading 0 stripped out
				if matchKey == "ZIP" && IsInt(column.Value) && len(column.Value) < 5 {
					column.Value = LeftPad2Len(column.Value, "0", 5)
				}
			} else if column.PeopleVER.IS_STREET1 && column.PeopleERR.Junk == 0 {
				if dev {
					log.Printf("VER ADDRESS1 & !Junk: %v %v %v", column.Name, column.Value, input.Signature.EventID)
				}
				SetMkField(&mkOutput, "AD1", column.Value, column.Name)
			} else if column.PeopleVER.IS_STREET2 && column.PeopleERR.Junk == 0 {
				if dev {
					log.Printf("VER ADDRESS2 & !Junk: %v %v %v", column.Name, column.Value, input.Signature.EventID)
				}
				SetMkField(&mkOutput, "AD2", column.Value, column.Name)
			} else if column.PeopleVER.IS_STREET3 && column.PeopleERR.Junk == 0 {
				if dev {
					log.Printf("VER ADDRESS3 & !Junk: %v %v %v", column.Name, column.Value, input.Signature.EventID)
				}
				SetMkField(&mkOutput, "AD3", column.Value, column.Name)
			} else if column.PeopleVER.IS_CITY && column.PeopleERR.Junk == 0 && column.PeopleERR.ContainsFirstName == 0 && column.PeopleERR.ContainsLastName == 0 && column.PeopleERR.MiddleName == 0 && column.PeopleERR.Gender == 0 {
				if dev {
					log.Printf("VER CITY & !Junk, Fname, Lname, Mname ERR: %v %v %v", column.Name, column.Value, input.Signature.EventID)
				}
				SetMkField(&mkOutput, "CITY", column.Value, column.Name)
			} else if column.PeopleVER.IS_STATE && column.PeopleERR.Junk == 0 && column.PeopleERR.MiddleName == 0 {
				if dev {
					log.Printf("VER STATE: %v %v %v", column.Name, column.Value, input.Signature.EventID)
				}
				SetMkField(&mkOutput, "STATE", column.Value, column.Name)
			} else if column.PeopleVER.IS_ZIPCODE && column.PeopleERR.ContainsZipCode == 1 && column.PeopleERR.Junk == 0 {
				if dev {
					log.Printf("VER ZIP: %v %v %v", column.Name, column.Value, input.Signature.EventID)
				}
				SetMkField(&mkOutput, "ZIP", column.Value, column.Name)
			} else if column.PeopleVER.IS_COUNTRY {
				if dev {
					log.Printf("VER COUNTRY: %v %v %v", column.Name, column.Value, input.Signature.EventID)
				}
				SetMkField(&mkOutput, "COUNTRY", column.Value, column.Name)
			} else if column.PeopleERR.ContainsFirstName == 1 {
				if dev {
					log.Printf("FName with loose ERR: %v %v %v", column.Name, column.Value, input.Signature.EventID)
				}
				SetMkField(&mkOutput, "FNAME", column.Value, column.Name)
			} else if column.PeopleERR.ContainsLastName == 1 {
				if dev {
					log.Printf("LName with loose ERR: %v %v %v", column.Name, column.Value, input.Signature.EventID)
				}
				SetMkField(&mkOutput, "LNAME", column.Value, column.Name)
			} else if column.PeopleERR.ContainsAddress == 1 {
				if dev {
					log.Printf("Ad1 with loose ERR: %v %v %v", column.Name, column.Value, input.Signature.EventID)
				}
				SetMkField(&mkOutput, "AD1", column.Value, column.Name)
			} 
			
		} else if column.PeopleERR.ContainsRole == 1 {
	// ************ check mpr second
			if dev { log.Printf("Non people role: %v", input.Signature.EventID) }
			
			if column.PeopleERR.ParentName == 1 {
				fullName = checkSetFullName(&mkOutput, mpr[memNumb])	
			} else if column.PeopleERR.ParentFirstName == 1 || (column.PeopleVER.IS_FIRSTNAME && column.PeopleERR.ContainsFirstName == 1) {
				if dev { log.Printf("Parent ERR FName or with VER & loose ERR: %v %v %v", column.Name, column.Value, input.Signature.EventID) }
				mpr[memNumb].FNAME.Value = column.Value
				mpr[memNumb].FNAME.Source = column.Name
			} else if column.PeopleERR.ParentLastName == 1 || (column.PeopleVER.IS_LASTNAME && column.PeopleERR.ContainsLastName == 1) {
				if dev { log.Printf("Parent ERR LName or with VER & loose ERR: %v %v %v", column.Name, column.Value, input.Signature.EventID) }
				mpr[memNumb].LNAME.Value = column.Value
				mpr[memNumb].LNAME.Source = column.Name
			} else if column.PeopleVER.IS_STREET1 && column.PeopleERR.ContainsAddress == 1 {
				if dev { log.Printf("Parent AD1 Ver & loose ERR: %v %v %v", column.Name, column.Value, input.Signature.EventID) }
				// look for a specifically called out MPR address
				mpr[memNumb].AD1.Value = column.Value
				mpr[memNumb].AD1.Source = column.Name
			} else if column.PeopleVER.IS_STREET2 && column.PeopleERR.ContainsAddress == 1 {
				if dev { log.Printf("Parent AD2 Ver & loose ERR: %v %v %v", column.Name, column.Value, input.Signature.EventID) }
				mpr[memNumb].AD2.Value = column.Value
				mpr[memNumb].AD2.Source = column.Name
			} else if column.PeopleVER.IS_CITY && column.PeopleERR.ContainsCity == 1 {
				if dev { log.Printf("Parent CITY Ver & loose ERR: %v %v %v", column.Name, column.Value, input.Signature.EventID) }
				mpr[memNumb].CITY.Value = column.Value
				mpr[memNumb].CITY.Source = column.Name
			} else if column.PeopleVER.IS_STATE && column.PeopleERR.ContainsState == 1 {
				if dev { log.Printf("Parent STATE Ver & loose ERR: %v %v %v", column.Name, column.Value, input.Signature.EventID) }
				mpr[memNumb].STATE.Value = column.Value
				mpr[memNumb].STATE.Source = column.Name
			} else if column.PeopleVER.IS_ZIPCODE && column.PeopleERR.ContainsZipCode == 1 {
				if dev { log.Printf("Parent ZIP Ver & loose ERR: %v %v %v", column.Name, column.Value, input.Signature.EventID) }
				mpr[memNumb].ZIP.Value = column.Value
				mpr[memNumb].ZIP.Source = column.Name
			} else if column.PeopleVER.IS_COUNTRY && column.PeopleERR.ContainsCountry == 1 {
				if dev { log.Printf("Parent COUNTRY Ver & loose ERR: %v %v %v", column.Name, column.Value, input.Signature.EventID) }
				mpr[memNumb].COUNTRY.Value = column.Value
				mpr[memNumb].COUNTRY.Source = column.Name
			} else if column.PeopleVER.IS_EMAIL {
				if dev { log.Printf("Parent EMAIL Ver: %v %v %v", column.Name, column.Value, input.Signature.EventID) }
				mpr[memNumb].EMAIL.Value = column.Value
				mpr[memNumb].EMAIL.Source = column.Name
				if len(mkOutput.EMAIL.Value) > 0 {
					email := strings.ToLower(mkOutput.EMAIL.Value)
					if strings.HasSuffix(email, "gmail.com") || strings.HasSuffix(email, "yahoo.com") || strings.HasSuffix(email, "hotmail.com") {
						mpr[memNumb].EMAIL.Type = "Private"
					}
				}
			} else if column.PeopleVER.IS_PHONE && len(column.Value) >= 10 {
				numberValue := reNumberOnly.ReplaceAllString(column.Value, "")
				if len(numberValue) == 10 || (len(numberValue) == 11 && strings.HasPrefix(numberValue, "1")) {
					if dev { log.Printf("Parent PHONE Ver: %v %v %v", column.Name, column.Value, input.Signature.EventID) }
					mpr[memNumb].PHONE.Value = column.Value
					mpr[memNumb].PHONE.Source = column.Name
				}
			} else if column.PeopleERR.ContainsName == 1  { 
				// already in role = 1 branch...
				// at this point, have already hopefully cleared the parent fname, lname...
// fullName = checkSetFullName(&mkOutput, column)
			}
		} else if matchKey != "" {
			// if NOTHING else has been set... give the model a try...
			if len(GetMkField(&mkOutput, matchKey).Value) == 0 {
				log.Printf("trying to fill in blank field...: ")
				SetMkField(&mkOutput, matchKey, column.Value, column.Name)
			}
		}

		input.Columns[index] = column
	}

	AddressParse(&mkOutput, &input, concatCityState, concatCityStateCol, concatAdd, concatAddCol)

	// check zip city state match
	ZipCheck := CheckCityStateZip(mkOutput.CITY.Value, mkOutput.STATE.Value, mkOutput.ZIP.Value)
	// >>>>>disable during development...
	if ZipCheck == false && len(mkOutput.AD1.Value) > 0 && false {
		address := strings.Join([]string{mkOutput.AD1.Value, mkOutput.AD2.Value, mkOutput.CITY.Value, mkOutput.STATE.Value, mkOutput.ZIP.Value}, ",")
		correctedOutputAddress := CorrectAddress(address)
		if len(correctedOutputAddress) > 0 {
			mkOutput.ADCORRECT.Value = "ZipCheck"
			mkOutput.AD1.Value = strings.Join([]string{correctedOutputAddress[0].Components.PrimaryNumber, " ", correctedOutputAddress[0].Components.StreetPredirection, " ", correctedOutputAddress[0].Components.StreetName, " ", correctedOutputAddress[0].Components.StreetSuffix}, "")
			mkOutput.AD2.Value = strings.Join([]string{correctedOutputAddress[0].Components.SecondaryDesignator, " ", correctedOutputAddress[0].Components.SecondaryNumber}, "")
			mkOutput.CITY.Value = correctedOutputAddress[0].Components.CityName
			mkOutput.STATE.Value = correctedOutputAddress[0].Components.StateAbbreviation
			mkOutput.ZIP.Value = correctedOutputAddress[0].Components.Zipcode
		}
	}

// TODO: check for overriding default value... 2x title values
	if roleCount > 0 {
		// what do we do... when dafault role was submitted?  in our case... take the max value?
	}

	// pub the record
	log.Printf("pubbing student...: %v", mkOutput)
	pubRecord(ctx, &input, mkOutput)

	// handle MAR values
	if emailCount > 1 {
		if dev {
			log.Printf("Have multiple emails: %v", emailCount)
		}
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
			if dev {
				log.Printf("pubbing MAR email %v ", mkOutput.EMAIL.Value)
			}
			pubRecord(ctx, &input, mkOutput)
		}
	}
	if phoneCount > 1 {
		if dev {
			log.Printf("Have multiple phones: %v", phoneCount)
		}
		for i := 1; i < len(phoneList); i++ {
			// update phone value... and resend...
			mkOutput.PHONE.Value = input.Columns[phoneList[i]].Value
			mkOutput.PHONE.Source = input.Columns[phoneList[i]].Name
			if dev {
				log.Printf("pubbing MAR phone %v ", mkOutput.PHONE.Value)
			}
			pubRecord(ctx, &input, mkOutput)
		}
	}
	if haveDorm {
		mkOutput.AD1.Value = input.Columns[dormCol].Value
		mkOutput.AD1.Source = input.Columns[dormCol].Name
		if roomCol > 0 {
			mkOutput.AD2.Value = input.Columns[roomCol].Name + ": " + input.Columns[roomCol].Value
			mkOutput.AD2.Source = input.Columns[roomCol].Name
		} else {
			mkOutput.AD2.Value = ""
		}
		mkOutput.CITY.Value = ""
		mkOutput.STATE.Value = ""
		mkOutput.ZIP.Value = ""
		mkOutput.ADTYPE.Value = "Campus"
		if dev {
			log.Printf("pubbing Dorm %v ", input.Columns[dormCol].Name)
		}
		pubRecord(ctx, &input, mkOutput)
	}

	// handle mpr
	for i := 0; i < len(mpr); i++ {
		if dev {
			log.Printf("mpr loop %v", i)
		}
		// if dev { log.Printf("will generate mpr if it has fname, email %v %v", mpr[i].FNAME.Value, mpr[i].EMAIL.Value) }
		if (mpr[i].FNAME.Value != "") || (mpr[i].EMAIL.Value != "") {
			if dev {
				log.Printf("have mpr value %v", i)
			}
			if mpr[i].FNAME.Value != "" {
				SetMkField(&mkOutput, "FNAME", mpr[i].FNAME.Value, mpr[i].FNAME.Source)
			} else {
				SetMkField(&mkOutput, "FNAME", "", "")
			}
			if mpr[i].LNAME.Value != "" {
				SetMkField(&mkOutput, "LNAME", mpr[i].LNAME.Value, mpr[i].LNAME.Source)
			}
			if mpr[i].AD1.Value != "" {
				SetMkField(&mkOutput, "AD1", mpr[i].AD1.Value, mpr[i].AD1.Source)
			}
			if mpr[i].AD2.Value != "" {
				SetMkField(&mkOutput, "AD2", mpr[i].AD2.Value, mpr[i].AD2.Source)
			}
			if mpr[i].AD3.Value != "" {
				SetMkField(&mkOutput, "AD3", mpr[i].AD3.Value, mpr[i].AD3.Source)
			}
			if mpr[i].CITY.Value != "" {
				SetMkField(&mkOutput, "CITY", mpr[i].CITY.Value, mpr[i].CITY.Source)
			}
			if mpr[i].STATE.Value != "" {
				SetMkField(&mkOutput, "STATE", mpr[i].STATE.Value, mpr[i].STATE.Source)
			}
			if mpr[i].ZIP.Value != "" {
				SetMkField(&mkOutput, "ZIP", mpr[i].ZIP.Value, mpr[i].ZIP.Source)
			}
			if mpr[i].EMAIL.Value != "" {
				SetMkField(&mkOutput, "EMAIL", mpr[i].EMAIL.Value, mpr[i].EMAIL.Source)
			} else {
				SetMkField(&mkOutput, "EMAIL", "", "")
			}
			if mpr[i].PHONE.Value != "" {
				SetMkField(&mkOutput, "PHONE", mpr[i].PHONE.Value, mpr[i].PHONE.Source)
			} else {
				SetMkField(&mkOutput, "PHONE", "", "")
			}

			addressInput := mkOutput.AD1.Value + " " + mkOutput.AD2.Value + " " + mkOutput.CITY.Value + " " + mkOutput.STATE.Value + " " + mkOutput.ZIP.Value
			if len(strings.TrimSpace(addressInput)) > 0 {
				a := ParseAddress(addressInput)
				if dev {
					log.Printf("mpr address parser returned %v", a)
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
						SetMkField(&mkOutput, "COUNTRY", "US", "WM")
					}
				}
			}

			mkOutput.ROLE.Value = "parent" // this should be generalized
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
	if dev {
		log.Printf("SetMkField: %v %v %v", field, value, source)
	}
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

func SetLibPostalField(v *LibPostalParsed, field string, value string) string {
	r := reflect.ValueOf(v)
	f := reflect.Indirect(r).FieldByName(field)
	f.SetString(value)
	return value
}

func checkSetFullName(mko *PeopleOutput, col InputColumn) bool {
	if dev {
		log.Printf("checking Full Name...")
	}
	if col.PeopleERR.FullName == 1 || (col.PeopleVER.IS_FIRSTNAME && col.PeopleVER.IS_LASTNAME && ((col.PeopleERR.ContainsFirstName == 1 && col.PeopleERR.ContainsLastName == 1) || (col.PeopleERR.ContainsFirstName == 0 && col.PeopleERR.ContainsLastName == 0))) {
		nameParts := strings.Split(col.Value, " ")
		if len(nameParts) > 1 {
			if dev {
				log.Printf("have multi-name...")
			}
			if strings.Contains(nameParts[0], ",") {
				commaLess := strings.Replace(nameParts[0], ",", "", 1)
				SetMkField(mko, "FNAME", strings.Join(nameParts[1:], ""), col.Name)
				SetMkField(mko, "LNAME", commaLess, col.Name)
				if dev {
					log.Printf("commaLess...: %v ", commaLess)
				}
			} else {
				SetMkField(mko, "FNAME", nameParts[0], col.Name)
				SetMkField(mko, "LNAME", strings.Join(nameParts[1:], " "), col.Name)
				if dev {
					log.Printf("fullname name: %v ", nameParts[0])
				}
			}
			return true
		}
	}
	return false
}

func calcClassYear(cy string) string {
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

func calcClassDesig(cy string) string {
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
