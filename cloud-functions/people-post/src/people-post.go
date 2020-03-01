package peoplepost

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/datastore"
	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"

	"github.com/gomodule/redigo/redis"
	"github.com/google/uuid"
)

var Env = os.Getenv("ENVIRONMENT")
var dev = Env == "dev"
var SmartyStreetsEndpoint = os.Getenv("SMARTYSTREET")
var AddressParserBaseUrl = os.Getenv("ADDRESSURL")
var AddressParserPath = os.Getenv("ADDRESSPATH")

var StorageBucket = os.Getenv("CLOUDSTORAGE")

var reGraduationYear = regexp.MustCompile(`20^\d{2}$`)
var reGraduationYear2 = regexp.MustCompile(`^\d{2}$`)
var reClassYearFY1 = regexp.MustCompile(`^FY\d{4}$`)
var reNumberOnly = regexp.MustCompile("[^0-9]+")
var reConcatenatedAddress = regexp.MustCompile(`(\d*)\s+((?:[\w+\s*\-])+)[\,]\s+([a-zA-Z]+)\s+([0-9a-zA-Z]+)`)
var reConcatenatedCityStateZip = regexp.MustCompile(`((?:[\w+\s*\-])+)[\,]\s+([a-zA-Z]+)\s+([0-9a-zA-Z]+)`)
var reNewline = regexp.MustCompile(`\r?\n`)
var reResidenceHall = regexp.MustCompile(`(?i)\sALPHA|ALUMNI|APARTMENT|APTS|BETA|BUILDING|CAMPUS|CENTENNIAL|CENTER|CHI|COLLEGE|COMMON|COMMUNITY|COMPLEX|COURT|CROSS|DELTA|DORM|EPSILON|ETA|FOUNDER|FOUNTAIN|FRATERNITY|GAMMA|GARDEN|GREEK|HALL|HEIGHT|HERITAGE|HIGH|HILL|HOME|HONOR|HOUS|INN|INTERNATIONAL|IOTA|KAPPA|LAMBDA|LANDING|LEARNING|LIVING|LODGE|MEMORIAL|MU|NU|OMEGA|OMICRON|PARK|PHASE|PHI|PI|PLACE|PLAZA|PSI|RESIDEN|RHO|RIVER|SCHOLARSHIP|SIGMA|SQUARE|STATE|STUDENT|SUITE|TAU|TERRACE|THETA|TOWER|TRADITIONAL|UNIV|UNIVERSITY|UPSILON|VIEW|VILLAGE|VISTA|WING|WOOD|XI|YOUNG|ZETA`)
var reState = regexp.MustCompile(`(?i)^(AL|AK|AZ|AR|CA|CO|CT|DC|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|PR|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY)$`)
var reStateFull = regexp.MustCompile(`(?i)^(alabama|alaska|arizona|arkansas|california|colorado|connecticut|delaware|district of columbia|florida|georgia|hawaii|idaho|illinois|indiana|iowa|kansas|kentucky|louisiana|maine|maryland|massachusetts|michigan|minnesota|mississippi|missouri|montana|nebraska|nevada|new hampshire|new jersey|new mexico|new york|north carolina|north dakota|ohio|oklahoma|oregon|pennsylvania|rhode island|south carolina|south dakota|tennessee|texas|utah|vermont|virginia|washington|west virginia|wisconsin|wyoming)$`)
var reOverseasBaseState = regexp.MustCompile(`(?i)^(AA|AE|AP)$`)
var reFullName = regexp.MustCompile(`^(.+?) ([^\s,]+)(,? (?:[JS]r\.?|III?|IV))?$`)
var reFullName2 = regexp.MustCompile(`^(.*), (.*) (.{1})\.$`) // Wilson, Lauren K.
var reFullName3 = regexp.MustCompile(`^(.*), (.*)$`)          // Wilson, Lauren K.
var reNameTitle = regexp.MustCompile(`(?i)^(mr|ms|miss|mrs|dr|mr\.|ms\.|dr\.|miss|mrs\.|Mr\.|Ms\.|Mrs\.|MR|MRS|MS)$`)

var fieldsToCopyForDefault = []string{"AD1", "AD2", "AD1NO", "ADTYPE", "ADBOOK", "CITY", "STATE", "ZIP", "COUNTRY", "ZIPTYPE", "RECORDTYPE", "ADPARSER"}

var redisTransientExpiration = 3600 * 24
var redisTemporaryExpiration = 3600

var DSKindSet = os.Getenv("DSKINDSET")
var DSKindGolden = os.Getenv("DSKINDGOLDEN")
var DSKindFiber = os.Getenv("DSKINDFIBER")

// JY: this code looks dangerous as it uses contains, think minneapolis
func reMilityBaseCity(val string) bool {
	city := strings.ToUpper(val)
	if city == "AFB" || city == "APO" || city == "DPO" || city == "FPO" {
		return true
	}
	// if strings.Contains(key, "AFB") || strings.Contains(key, "APO") || strings.Contains(key, "DPO") || strings.Contains(key, "FPO") {
	// 	return true
	// }
	return false
}

var zipMap map[string]CityState // intended to be part of address correction
var ps *pubsub.Client
var topic *pubsub.Topic
var topic2 *pubsub.Topic
var expire *pubsub.Topic
var martopic *pubsub.Topic
var ap http.Client
var sb *storage.Client
var msp *redis.Pool
var fs *datastore.Client

var MLLabels map[string]string

func init() {
	ctx := context.Background()
	ps, _ = pubsub.NewClient(ctx, os.Getenv("PROJECTID"))
	fs, _ = datastore.NewClient(ctx, os.Getenv("DSPROJECTID"))
	topic = ps.Topic(os.Getenv("PSOUTPUT"))
	martopic = ps.Topic(os.Getenv("PSOUTPUT"))
	expire = ps.Topic(os.Getenv("PSOUTPUT"))
	// martopic.PublishSettings.DelayThreshold = 1 * time.Second
	MLLabels = map[string]string{"0": "", "1": "AD1", "2": "AD2", "3": "CITY", "4": "COUNTRY", "5": "EMAIL", "6": "FNAME", "7": "LNAME", "8": "PHONE", "9": "STATE", "10": "ZIP"}
	sb, _ := storage.NewClient(ctx)
	zipMap, _ = readZipMap(ctx, sb, StorageBucket, "data/zip_city_state.json") // intended to be part of address correction
	ap = http.Client{
		Timeout: time.Second * 2, // Maximum of 2 secs
	}

	msp = &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial:        func() (redis.Conn, error) { return redis.Dial("tcp", os.Getenv("MEMSTORE")) },
	}

	log.Printf("init completed, pubsub topic name: %v, zipmap size %v", topic, len(zipMap))
}

var TitleYear int

func PostProcessPeople(ctx context.Context, m PubSubMessage) error {
	var input Input
	// log.Printf("Received message %+v", string(m.Data))
	if err := json.Unmarshal(m.Data, &input); err != nil {
		log.Fatalf("Unable to unmarshal message %v with error %v", string(m.Data), err)
	}

	TitleYear = time.Now().Year()
	var MPRCounter int       // keep track of how many MPR we have
	var MARCounter int       // keep track of how many MAR we have
	var outputs []PostRecord // this contains all outputs with a type

	LogDev(fmt.Sprintf("people-post for record: %v", input.Signature.RecordID))

	// locate title year, if a 4 digit year is passed then assign it
	for _, column := range input.Columns {
		if strings.ToLower(column.Name) == "titleyear" && column.IsAttribute {
			// make sure thie field is not used for anything else
			LogDev(fmt.Sprintf("Using attribute TitleYear: %v", column.Value))
			column.PeopleERR.Junk = 1

			if strings.HasPrefix(column.Value, "20") && len(column.Value) == 4 && IsInt(column.Value) {
				titleYear, _ := strconv.ParseInt(column.Value, 10, 0)
				TitleYear = int(titleYear)
			}
		}
	}

	// iterate through every column on the input record to decide what the column is...
	for _, column := range input.Columns {
		// start with some sanitization
		column.Value = strings.TrimSpace(column.Value)
		column.Value = reNewline.ReplaceAllString(column.Value, " ")
		column.Value = strings.Replace(column.Value, "  ", " ", -1)
		column.Value = strings.Replace(column.Value, "  ", " ", -1)
		if len(column.Value) == 0 { //dont need to work with blank values
			continue
		}

		if strings.ToLower(column.Name) == "titleyear" && column.IsAttribute {
			// make sure thie field is not used for anything else
			column.PeopleERR.Junk = 1
		}

		// capture ML prediction to column
		// predictionValue := input.Prediction.Predictions[index]
		// predictionKey := strconv.Itoa(int(predictionValue))
		// mlMatchKey := MLLabels[predictionKey]
		// column.MatchKey = mlMatchKey

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
			// TODO: a contains here seems VERY dangerous...
			column.MatchKey1 = "ROLE"
			LogDev(fmt.Sprintf("MatchKey %v on condition %v", column.MatchKey1, "column.PeopleERR.ContainsStudentRole == 1"))
		} else if (column.PeopleERR.Title == 1 && !reNameTitle.MatchString(column.Value)) || (column.PeopleERR.ContainsTitle == 1 && !reNameTitle.MatchString(column.Value)) {
			column.MatchKey1 = "TITLE"
			column.MatchKey2 = "STATUS"
			LogDev(fmt.Sprintf("MatchKey %v on condition %v and %v", column.MatchKey1, column.MatchKey2, " column.PeopleERR.Title == 1 || column.PeopleERR.ContainsTitle == 1"))
			column.MatchKey = ""
			column.PeopleERR.Country = 0 // override this is NOT a country
			column.PeopleERR.State = 0   // override this is NOT a state value
		} else if column.PeopleERR.Dorm == 1 && reResidenceHall.MatchString(column.Value) {
			// TODO: come back and fix this... maybe drop MAR all together?
			// column.MatchKey1 = "DORM"
			LogDev(fmt.Sprintf("MatchKey %v on condition %v", column.MatchKey1, "column.PeopleERR.Dorm == 1 && reResidenceHall.MatchString(column.Value)"))
		} else if column.PeopleERR.Room == 1 {
			// TODO: come back and fix this... maybe drop MAR all together?
			// column.MatchKey1 = "ROOM"
			LogDev(fmt.Sprintf("MatchKey %v on condition %v", column.MatchKey1, "column.PeopleERR.Room == 1"))
		} else if column.PeopleERR.FullAddress == 1 {
			column.MatchKey1 = "FULLADDRESS"
			LogDev(fmt.Sprintf("MatchKey %v on condition %v", column.MatchKey1, "column.PeopleERR.FullAddress == 1"))
		} else if column.PeopleERR.ContainsCity == 1 && (column.PeopleERR.ContainsState == 1 || column.PeopleERR.ContainsZipCode == 1) {
			column.MatchKey1 = "CITYSTATEZIP"
			LogDev(fmt.Sprintf("MatchKey %v on condition %v", column.MatchKey1, "column.PeopleERR.ContainsState == 1 && column.PeopleERR.ContainsCity == 1"))
		}

		var parsedName NameParsed
		// this might be a full name, try to parse it and see if we have first and last names
		// || (column.PeopleVER.IS_FIRSTNAME && column.PeopleVER.IS_LASTNAME && column.PeopleERR.ContainsName == 1)
		if column.PeopleERR.ContainsRole == 1 || column.PeopleERR.FullName == 1 || (column.PeopleVER.IS_FIRSTNAME && column.PeopleVER.IS_LASTNAME && ((column.PeopleERR.ContainsFirstName == 1 && column.PeopleERR.ContainsLastName == 1) || (column.PeopleERR.ContainsFirstName == 0 && column.PeopleERR.ContainsLastName == 0))) {
			parsedName = ParseName(column.Value)
			if len(parsedName.FNAME) > 0 && len(parsedName.LNAME) > 0 && column.PeopleERR.Address == 0 && column.PeopleERR.Address1 == 0 && column.PeopleERR.ContainsAddress == 0 && column.PeopleERR.City == 0 && column.PeopleERR.ContainsCity == 0 {
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
			} else if column.PeopleVER.IS_COUNTRY && (column.PeopleERR.Country == 1 || column.PeopleERR.Address2 == 1 || column.PeopleERR.Address3 == 1 || column.PeopleERR.Address4 == 1) {
				column.MatchKey1 = "COUNTRY"
				LogDev(fmt.Sprintf("MatchKey %v on condition %v", column.MatchKey1, "column.PeopleVER.IS_COUNTRY && column.PeopleERR.Country == 1"))
			} else if column.PeopleVER.IS_EMAIL {
				column.MatchKey1 = "EMAIL"
				LogDev(fmt.Sprintf("MatchKey %v on condition %v", column.MatchKey1, "column.PeopleVER.IS_EMAIL"))
			} else if column.PeopleVER.IS_PHONE && len(column.Value) >= 10 {
				numberValue := reNumberOnly.ReplaceAllString(column.Value, "")
				if column.PeopleERR.Phone == 1 && (len(numberValue) == 10 || (len(numberValue) == 11 && strings.HasPrefix(numberValue, "1"))) { // only handle US phone format
					column.MatchKey1 = "PHONE"
					LogDev(fmt.Sprintf("MatchKey %v on condition %v", column.MatchKey1, "column.PeopleVER.IS_PHONE && len(column.Value) >= 10"))
				}
			} else if column.PeopleERR.ContainsFirstName == 1 && column.PeopleVER.IS_FIRSTNAME {
				column.MatchKey1 = "FNAME"
				LogDev(fmt.Sprintf("MatchKey %v on condition %v", column.MatchKey1, "column.PeopleERR.ContainsFirstName == 1 && column.PeopleVER.IS_FIRSTNAME"))
			} else if column.PeopleERR.FirstName == 1 {
				column.MatchKey1 = "FNAME"
				LogDev(fmt.Sprintf("MatchKey %v on condition %v", column.MatchKey1, "column.PeopleERR.FirstName == 1"))
			} else if column.PeopleERR.MiddleName == 1 {
				column.MatchKey1 = "MNAME"
				LogDev(fmt.Sprintf("MatchKey %v on condition %v", column.MatchKey1, "column.PeopleERR.MiddleName == 1"))
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
			} else if column.PeopleERR.State == 1 || (column.PeopleERR.ContainsRole == 1 && column.PeopleERR.ContainsState == 1) {
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
			} else if column.PeopleVER.IS_STATE && column.PeopleERR.Junk == 0 && column.PeopleERR.MiddleName == 0 {
				column.MatchKey1 = "STATE"
				LogDev(fmt.Sprintf("MatchKey %v on condition %v", column.MatchKey1, "column.PeopleVER.IS_STATE && column.PeopleERR.Junk == 0 && column.PeopleERR.MiddleName == 0"))
			} else if column.PeopleVER.IS_ZIPCODE && column.PeopleERR.ContainsZipCode == 1 && column.PeopleERR.Junk == 0 {
				column.MatchKey1 = "ZIP"
				LogDev(fmt.Sprintf("MatchKey %v on condition %v", column.MatchKey1, "column.PeopleVER.IS_ZIPCODE && column.PeopleERR.ContainsZipCode == 1 && column.PeopleERR.Junk == 0"))
			} else if column.PeopleVER.IS_CITY && column.PeopleERR.Junk == 0 && column.PeopleERR.ContainsFirstName == 0 && column.PeopleERR.ContainsLastName == 0 && column.PeopleERR.MiddleName == 0 && column.PeopleERR.Gender == 0 && column.PeopleERR.ContainsRole == 0 {
				column.MatchKey1 = "CITY"
				LogDev(fmt.Sprintf("MatchKey %v on condition %v", column.MatchKey1, "column.PeopleVER.IS_CITY && column.PeopleERR.Junk == 0 && column.PeopleERR.ContainsFirstName == 0 && column.PeopleERR.ContainsLastName == 0 && column.PeopleERR.MiddleName == 0 && column.PeopleERR.Gender == 0"))
			} else if column.PeopleVER.IS_COUNTRY {
				column.MatchKey1 = "COUNTRY"
				LogDev(fmt.Sprintf("MatchKey %v on condition %v", column.MatchKey1, "column.PeopleVER.IS_COUNTRY"))
			} else if column.PeopleERR.ContainsFirstName == 1 {
				column.MatchKey1 = "FNAME"
				LogDev(fmt.Sprintf("MatchKey %v on condition %v", column.MatchKey1, "column.PeopleERR.ContainsFirstName == 1"))
			} else if column.PeopleERR.ContainsLastName == 1 {
				column.MatchKey1 = "LNAME"
				LogDev(fmt.Sprintf("MatchKey %v on condition %v", column.MatchKey1, "column.PeopleERR.ContainsLastName == 1"))
			} else if column.PeopleERR.ContainsCity == 1 && column.PeopleERR.Junk == 0 && column.PeopleERR.Gender == 0 {
				column.MatchKey1 = "CITY"
				LogDev(fmt.Sprintf("MatchKey %v on condition %v", column.MatchKey1, "column.PeopleERR.ContainsCity == 1 && column.PeopleERR.Junk == 0 && column.PeopleERR.Gender == 0"))
			} else if column.PeopleERR.ContainsAddress == 1 {
				column.MatchKey1 = "AD1"
				LogDev(fmt.Sprintf("MatchKey %v on condition %v", column.MatchKey1, "column.PeopleERR.ContainsAddress == 1"))
			}
		}

		if reMilityBaseCity(column.Value) {
			column.MatchKey1 = "CITY"
			LogDev(fmt.Sprintf("overriding city by military base: %v", column.Value))
		}

		if reOverseasBaseState.MatchString(column.Value) {
			column.MatchKey1 = "STATE"
			LogDev(fmt.Sprintf("overriding state by USPS base designation: %v", column.Value))
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
			if column.Value == "true" || column.Value == "false" || column.Value == "0001-01-01T00:00:00+00:00" || column.Value == "0.000000" {
				column.MatchKey = ""
				column.MatchKey1 = ""
				column.MatchKey2 = ""
				column.MatchKey3 = ""
			} else {
				column.Type = AssignAddressType(&column)
				column.MatchKey2 = "ADTYPE"
				column.MatchKey3 = "ADBOOK"
			}
		}

		// clear MatchKey if Junk
		if column.PeopleERR.Junk == 1 {
			LogDev(fmt.Sprintf("JUNK is dropping your match keys: %v %v %v %v", column.MatchKey, column.MatchKey1, column.MatchKey2, column.MatchKey3))
			column.MatchKey = ""
			column.MatchKey1 = ""
			column.MatchKey2 = ""
			column.MatchKey3 = ""
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
		skipValue := false
		if len(matchKeyAssigned) > 0 {
			if column.PeopleERR.ContainsRole == 0 { // not MPR
				currentOutput, indexOutput = GetOutputByType(&outputs, "default")
				if matchKeyAssigned == "DORM" || matchKeyAssigned == "ROOM" { // write out dorm address as a new output
					currentOutput, indexOutput = GetOutputByType(&outputs, "dorm")
				}
				// TODO: make sure to not overwrite title column with title attribute
				currentValue := GetMkField(&(currentOutput.Output), matchKeyAssigned)
				if matchKeyAssigned == "TITLE" {
					LogDev(fmt.Sprintf("pending title assignment - column %v, match key %v, isattribute %v, current value %v", column.Name, matchKeyAssigned, column.IsAttribute, currentValue))
				}
				// do not overwrite a matchkey from attribute if matchkey already as value assigned
				if column.IsAttribute && len(currentValue.Value) > 0 {
					skipValue = true
				} else { // skip the MAR if it is an attribute
					for {
						if len(currentValue.Value) == 0 {
							break
						}
						MARCounter++
						currentOutput, indexOutput = GetOutputByTypeAndSequence(&outputs, "mar", MARCounter)
						currentValue = GetMkField(&(currentOutput.Output), matchKeyAssigned)
					}
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

			if !skipValue {
				// let's assign the value
				switch matchKeyAssigned {
				case "TITLE":
					LogDev(fmt.Sprintf("ClassYear value %v, TitleYear value %v, final value %v", column.Value, TitleYear, CalcClassYear(column.Value)))
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
					SetMkField(&(currentOutput.Output), "ADBOOK", "Ship", column.Name)
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

				if len(column.MatchKey3) > 0 {
					SetMkField(&(currentOutput.Output), "ADBOOK", AssignAddressBook(&column), column.Name)
				}
				//columnOutput := *currentOutput
				outputs[indexOutput] = *currentOutput
			}
		} else {
			log.Printf("Event %v Record %v Column has no match key assigned: %v %v", input.Signature.EventID, input.Signature.RecordID, column.Name, column.Value)
		}
		LogDev(fmt.Sprintf("Outputs is %v", outputs))
		// input.Columns[index] = column // dont need to update the input
	}
	LogDev(fmt.Sprintf("Finishing with %v outputs", len(outputs)))

	defaultOutput, _ := GetOutputByType(&outputs, "default")

	// check to see if we need to deal with MAR that needs to be merged back to the default output
	// specifically we are checking if the MAR field is AD1 and if default has a blank AD2
	indexToSkip := -1

	defaultMissingAddress := false
	mprIndexWithAddress := -1

	// TODO: struggling to follow what this does.. I THINK what it was supposed to do was... if there was
	// a pobox in ad2... rahter than ad1... to flip flop them?
	for i, v := range outputs {
		if v.Type == "default" {
			ad2 := GetMkField(&(v.Output), "AD2")
			if len(ad2.Value) == 0 { // see if we have a MAR with AD1 only
				for j, o := range outputs {
					if o.Type == "mar" {
						populatedKeys := GetPopulatedMatchKeys(&(o.Output))
						if len(populatedKeys) == 1 && populatedKeys[0] == "AD1" {
							mar := GetMkField(&(o.Output), "AD1")
							SetMkField(&(v.Output), "AD2", mar.Value, mar.Source)
							outputs[i] = v
							LogDev(fmt.Sprintf("mar check assigned mar AD1 to default"))
							indexToSkip = j
						} else {
							LogDev(fmt.Sprintf("mar check returned list of populated keys: %v", populatedKeys))
						}
					}
				}
			}
			if len(GetMkField(&(v.Output), "AD1").Value) == 0 {
				defaultMissingAddress = true
			}
		} else if v.Type == "mpr" {
			if (len(GetMkField(&(v.Output), "AD1").Value) > 0 || len(GetMkField(&(v.Output), "AD2").Value) > 0) && mprIndexWithAddress == -1 {
				mprIndexWithAddress = i
			}
		}
	}
	LogDev(fmt.Sprintf("defaultMissingAddress = %v, mprIndexWithAddress = %v", defaultMissingAddress, mprIndexWithAddress))

	pubQueue := []PubQueue{}
	SetRedisValueWithExpiration([]string{input.Signature.EventID, input.Signature.RecordID, "fiber-mar-retry"}, 0)
	for i, v := range outputs {
		if i == indexToSkip {
			continue
		}
		LogDev(fmt.Sprintf("Pub output %v of %v, type %v, sequence %v: %v", i, len(outputs), v.Type, v.Sequence, v.Output))
		suffix := ""
		if v.Type == "mpr" {
			suffix = "-^-" + strconv.Itoa(v.Sequence)
			CopyFieldsToMPR(&(defaultOutput.Output), &(v.Output))
		}
		if len(v.Output.CITY.Value) == 0 && len(v.Output.STATE.Value) == 0 && len(v.Output.ZIP.Value) >= 5 { // let's populate city state if we have zip
			v.Output.CITY.Value, v.Output.STATE.Value = populateCityStateFromZip(v.Output.ZIP.Value)
			if len(v.Output.CITY.Value) > 0 || len(v.Output.STATE.Value) > 0 {
				v.Output.CITY.Source = "WM"
				v.Output.STATE.Source = "WM"
			}
		}

		// copy address fields from MPR to default if value is missing
		if v.Type == "default" && defaultMissingAddress && mprIndexWithAddress > -1 {
			for _, f := range fieldsToCopyForDefault {
				mk := GetMkField(&(outputs[mprIndexWithAddress].Output), f)
				mko := GetMkField(&(v.Output), f)
				if len(mko.Value) == 0 {
					SetMkField(&(v.Output), f, mk.Value, mk.Source)
				}
			}

		}

		// // do a state lookup, no longer necessary
		// stateUpper := strings.ToUpper(v.Output.STATE.Value)
		// if sa, ok := StateList[stateUpper]; ok {
		// 	v.Output.STATE.Value = sa
		// }

		// don't forget we can have email only records....

		// standardize state name > abreviation
		if reStateFull.MatchString(strings.ToLower(v.Output.STATE.Value)) {
			LogDev(fmt.Sprintf("standardizing STATE to 2 letter abbreviation: %v", v.Output.STATE.Value))
			v.Output.STATE.Value = lookupState(v.Output.STATE.Value)
		}
		// standardize "US" for any US State address
		if reState.MatchString(v.Output.STATE.Value) {
			LogDev(fmt.Sprintf("overriding country by state value: %v", v.Output.STATE.Value))
			v.Output.COUNTRY.Value = "US"
			v.Output.COUNTRY.Source = "WM"
		}

		// If we could not identify another country previously...
		// including US... meaning we don't have a state
		// yet we have an address
		if v.Output.AD1.Value != "" && v.Output.COUNTRY.Value == "" {
			LogDev(fmt.Sprintf("trying to find a country %v %v %v", v.Output.AD1.Value, v.Output.AD2.Value, v.Output.STATE.Value))
			if v.Output.STATE.Value == "other" {
				v.Output.COUNTRY.Value = "INTL"
			}
			// TODO: Jie can you look at this poop...
			// scan Ad1, Ad2, Ad3, Ad4... to see if we can find a country code...
		}

		// IF we believe it to NOT be an international address...
		// if v.Output.COUNTRY.Value == "US" || v.Output.COUNTRY.Value == "USA" || v.Output.COUNTRY.Value == "United States" || v.Output.COUNTRY.Value == "United States of America" || v.Output.COUNTRY.Value == "America" {
		if v.Output.COUNTRY.Value == "US" || v.Output.COUNTRY.Value == "" {
			v.Output.ADVALID.Value = "FALSE"
			v.Output.ADCORRECT.Value = "FALSE"
			StandardizeAddressSS(&(v.Output))
			// StandardizeAddressLP(&(v.Output)) // not using libpostal right now...
		}

		if v.Output.ADBOOK.Value == "" {
			v.Output.ADBOOK.Value = "Bill"
		}

		// SetMkField(&(currentOutput.Output), "ADBOOK", AssignAddressBook(&column), column.Name)

		if len(v.Output.FNAME.Value) > 0 {
			v.Output.FINITIAL = MatchKeyField{
				Value: v.Output.FNAME.Value[0:1],
			}
		}

		// MatchByValue1 := strings.Replace(v.Output.TRUSTEDID.Value, "'", `''`, -1)
		// MatchByValue2 := strings.Replace(v.Output.EMAIL.Value, "'", `''`, -1)
		// MatchByValue3A := strings.Replace(v.Output.PHONE.Value, "'", `''`, -1)
		// MatchByValue3B := strings.Replace(v.Output.FINITIAL.Value, "'", `''`, -1)
		// MatchByValue5A := strings.Replace(v.Output.CITY.Value, "'", `''`, -1)
		// MatchByValue5B := strings.Replace(v.Output.STATE.Value, "'", `''`, -1)
		// MatchByValue5C := strings.Replace(v.Output.LNAME.Value, "'", `''`, -1)
		// MatchByValue5D := strings.Replace(v.Output.FNAME.Value, "'", `''`, -1)
		// MatchByValue5E := strings.Replace(v.Output.AD1.Value, "'", `''`, -1)
		// // MatchByValue5F := strings.Replace(v.Output.ADBOOK.Value, "'", `''`, -1)

		// redisMatchValue0 := []string{input.Signature.EventID, input.Signature.RecordID, "match"}
		// SetRedisTempKey(append(redisMatchValue0, "retry"))
		// // SetRedisTempKey(redisMatchValue0)

		// if len(MatchByValue1) > 0 {
		// 	redisMatchValue1 := []string{input.Signature.EventID, strings.ToUpper(MatchByValue1), "match"}
		// 	SetRedisTempKey(append(redisMatchValue1, "retry"))
		// 	// SetRedisTempKey(redisMatchValue1)
		// }
		// if len(MatchByValue2) > 0 {
		// 	redisMatchValue2 := []string{input.Signature.EventID, strings.ToUpper(MatchByValue2), "match"}
		// 	SetRedisTempKey(append(redisMatchValue2, "retry"))
		// 	// SetRedisTempKey(redisMatchValue2)
		// }
		// if len(MatchByValue3A) > 0 && len(MatchByValue3B) > 0 {
		// 	redisMatchValue3 := []string{input.Signature.EventID, strings.ToUpper(MatchByValue3A), strings.ToUpper(MatchByValue3B), "match"}
		// 	SetRedisTempKey(append(redisMatchValue3, "retry"))
		// 	// SetRedisTempKey(redisMatchValue3)
		// }
		// if len(MatchByValue5A) > 0 && len(MatchByValue5B) > 0 && len(MatchByValue5C) > 0 && len(MatchByValue5D) > 0 && len(MatchByValue5E) > 0 {
		// 	redisMatchValue5 := []string{input.Signature.EventID, strings.ToUpper(MatchByValue5A), strings.ToUpper(MatchByValue5B), strings.ToUpper(MatchByValue5C), strings.ToUpper(MatchByValue5D), strings.ToUpper(MatchByValue5E), "match"}
		// 	SetRedisTempKey(append(redisMatchValue5, "retry"))
		// 	// SetRedisTempKey(redisMatchValue5)
		// }

		// allMatchKeys := []string{input.Signature.EventID, "dupe"}
		// for _, f := range structs.Names(&PeopleOutput{}) {
		// 	allMatchKeys = append(allMatchKeys, strings.ToUpper(GetMkField(&(v.Output), f).Value))
		// }
		// if GetRedisIntValue(allMatchKeys) > 0 {
		// 	LogDev(fmt.Sprintf("Detected Dupe value %v", allMatchKeys))
		// 	v.Type = "dupe"
		// } else {
		// 	SetRedisTempKey(allMatchKeys)
		// }

		// preload Set (Search:[FiberID])
		if len(input.Signature.RecordID) == 0 {
			// ensure record id is not blank or we'll have problem
			input.Signature.RecordID = uuid.New().String()
		}
		var searchFields []string
		searchFields = append(searchFields, fmt.Sprintf("RECORDID=%v", input.Signature.RecordID))
		if len(v.Output.EMAIL.Value) > 0 {
			searchFields = append(searchFields, fmt.Sprintf("EMAIL=%v", strings.TrimSpace(strings.ToUpper(v.Output.EMAIL.Value))))
		}
		if len(v.Output.PHONE.Value) > 0 && len(v.Output.FINITIAL.Value) > 0 {
			searchFields = append(searchFields, fmt.Sprintf("PHONE=%v&FINITIAL=%v", strings.TrimSpace(strings.ToUpper(v.Output.PHONE.Value)), strings.TrimSpace(strings.ToUpper(v.Output.FINITIAL.Value))))
		}
		if len(v.Output.CITY.Value) > 0 && len(v.Output.STATE.Value) > 0 && len(v.Output.LNAME.Value) > 0 && len(v.Output.FNAME.Value) > 0 && len(v.Output.AD1.Value) > 0 {
			searchFields = append(searchFields, fmt.Sprintf("FNAME=%v&LNAME=%v&AD1=%v&CITY=%v&STATE=%v", strings.TrimSpace(strings.ToUpper(v.Output.FNAME.Value)), strings.TrimSpace(strings.ToUpper(v.Output.LNAME.Value)), strings.TrimSpace(strings.ToUpper(v.Output.AD1.Value)), strings.TrimSpace(strings.ToUpper(v.Output.CITY.Value)), strings.TrimSpace(strings.ToUpper(v.Output.STATE.Value))))
		}

		dsNameSpace := strings.ToLower(fmt.Sprintf("%v-%v", Env, input.Signature.OwnerID))
		log.Printf("Searchfields %+v", searchFields)
		if len(searchFields) > 0 {
			for _, search := range searchFields {
				searchValue := strings.Replace(search, "'", `''`, -1)
				querySets := []PeopleSetDS{}
				if _, err := fs.GetAll(ctx, datastore.NewQuery(DSKindSet).Namespace(dsNameSpace).Filter("search =", searchValue), &querySets); err != nil {
					log.Fatalf("Error querying sets: %v", err)
				}
				var expiredSetCollection []string
				var matchedFibers []string
				log.Printf("Fiber type %v Search %v found %v sets", v.Type, search, len(querySets))
				for _, s := range querySets {
					if !Contains(expiredSetCollection, s.ID.Name) {
						expiredSetCollection = append(expiredSetCollection, s.ID.Name)
						log.Printf("Adding set %v to expired sets", s.ID.Name)
					}
					if len(s.Fibers) > 0 {
						for _, f := range s.Fibers {
							if !Contains(matchedFibers, f) {
								matchedFibers = append(matchedFibers, f)
							}
						}
					}
				}

				// load search keys into memstore, load existing first
				if len(matchedFibers) > 0 {
					msKey := []string{input.Signature.OwnerID, "search", search}
					for _, mf := range matchedFibers {
						AppendRedisTempKey(msKey, mf)
					}
				}

				log.Printf("Expiring sets %+v", expiredSetCollection)
				if len(expiredSetCollection) > 0 {
					msKey := []string{input.Signature.OwnerID, "set", search}
					for _, sk := range expiredSetCollection {
						AppendRedisTempKey(msKey, sk)
					}
				}
				// 	// remove expired sets and setmembers from DS
				// 	var SetKeys []*datastore.Key
				// 	// var MemberKeys []*datastore.Key
				// 	var GoldenKeys []*datastore.Key

				// 	for _, set := range expiredSetCollection {
				// 		setKey := datastore.NameKey(DSKindSet, set, nil)
				// 		setKey.Namespace = dsNameSpace
				// 		SetKeys = append(SetKeys, setKey)
				// 		goldenKey := datastore.NameKey(DSKindGolden, set, nil)
				// 		goldenKey.Namespace = dsNameSpace
				// 		GoldenKeys = append(GoldenKeys, goldenKey)
				// 	}

				// 	LogDev(fmt.Sprintf("deleting %v expired sets and %v expired golden records", len(SetKeys), len(GoldenKeys)))
				// 	if err := fs.DeleteMulti(ctx, SetKeys); err != nil {
				// 		log.Printf("Error: deleting expired sets: %v", err)
				// 	}
				// 	if err := fs.DeleteMulti(ctx, GoldenKeys); err != nil {
				// 		log.Printf("Error: deleting expired golden records: %v", err)
				// 	}

				// 	// expired := PeopleDelete{
				// 	// 	OwnerID: input.Signature.OwnerID,
				// 	// 	Expired: expiredSetCollection,
				// 	// }
				// 	// expireJSON, _ := json.Marshal(expired)
				// 	// expire.Publish(ctx, &pubsub.Message{
				// 	// 	Data: expireJSON,
				// 	// 	Attributes: map[string]string{
				// 	// 		"type":   "people",
				// 	// 		"source": "delete",
				// 	// 	},
				// 	// })
				// 	// log.Printf("pubbed delete %v", string(expireJSON))
				// }

			}
		}

		pubQueue = append(pubQueue, PubQueue{
			Output: v.Output,
			Suffix: suffix,
			Type:   v.Type,
		})
	}

	var pubs []Output
	for _, p := range pubQueue {
		var output Output
		output.Signature = input.Signature
		output.Signature.FiberType = p.Type
		if len(p.Suffix) > 0 {
			output.Signature.RecordID += p.Suffix
		}
		output.Passthrough = input.Passthrough
		output.MatchKeys = p.Output

		pubs = append(pubs, output)
	}
	outputJSON, _ := json.Marshal(pubs)
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
	return nil
}
