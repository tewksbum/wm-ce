package peoplepost

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/fatih/structs"

	"cloud.google.com/go/datastore"
	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"

	"googlemaps.github.io/maps"

	"github.com/gomodule/redigo/redis"
	"github.com/google/uuid"
)

var ProjectID = os.Getenv("PROJECTID")
var PubSubTopic = os.Getenv("PSOUTPUT")
var env = os.Getenv("ENVIRONMENT")
var dev = (env == "dev")
var SmartyStreetsEndpoint = os.Getenv("SMARTYSTREET")
var AddressParserBaseUrl = os.Getenv("ADDRESSURL")
var AddressParserPath = os.Getenv("ADDRESSPATH")

var StorageBucket = os.Getenv("CLOUDSTORAGE")
var cfName = os.Getenv("FUNCTION_NAME")

var reGraduationYear = regexp.MustCompile(`^20\d{2}$`)
var reGraduationYear2 = regexp.MustCompile(`^\d{2}$`)
var reClassYearFY1 = regexp.MustCompile(`^FY\d{4}$`)
var reZip5 = regexp.MustCompile(`^\d{5}$`)
var reZip9 = regexp.MustCompile(`^\d{5}-\d{4}$`)
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
var reFullName4 = regexp.MustCompile(`^(.*),(.*)$`)           // Wilson,Lauren
var reFullName5 = regexp.MustCompile(`^(.*),(.*)( .{1}\.)$`)  //// Wilson,Lauren K.
var reNameTitle = regexp.MustCompile(`(?i)^(mr|ms|miss|mrs|dr|mr\.|ms\.|dr\.|miss|mrs\.|Mr\.|Ms\.|Mrs\.|MR|MRS|MS)$`)

//To calculate class year and school status
var reFreshman = regexp.MustCompile(`(?i)freshman|frosh|fresh|fr|first year|new resident|1st year`)
var reSophomore = regexp.MustCompile(`(?i)sophomore|soph|so|2nd year`)
var reJunior = regexp.MustCompile(`(?i)junior|jr|3rd year`)
var reSenior = regexp.MustCompile(`(?i)senior|sr|4th year`)
var reGraduate = regexp.MustCompile(`(?i)^(graduate|undergraduate over 23 \(archive\)|gr)`)

var fieldsToCopyForDefault = []string{"AD1", "AD2", "AD1NO", "ADTYPE", "ADBOOK", "CITY", "STATE", "ZIP", "COUNTRY", "ZIPTYPE", "RECORDTYPE", "ADPARSER"}

var redisTransientExpiration = 3600 * 24
var redisTemporaryExpiration = 3600

// var StateList = map[string]string{
// 	"ALASKA": "AK", "ARIZONA": "AZ", "ARKANSAS": "AR", "CALIFORNIA": "CA", "COLORADO": "CO", "CONNECTICUT": "CT", "DELAWARE": "DE",
// 	"FLORIDA": "FL", "GEORGIA": "GA", "HAWAII": "HI", "IDAHO": "ID", "ILLINOIS": "IL", "INDIANA": "IN", "IOWA": "IA", "KANSAS": "KS",
// 	"KENTUCKY": "KY", "LOUISIANA": "LA", "MAINE": "ME", "MARYLAND": "MD", "MASSACHUSETTS": "MA", "MICHIGAN": "MI", "MINNESOTA": "MN",
// 	"MISSISSIPPI": "MS", "MISSOURI": "MO", "MONTANA": "MT", "NEBRASKA": "NE", "NEVADA": "NV", "NEW HAMPSHIRE": "NH", "NEW JERSEY": "NJ",
// 	"NEW MEXICO": "NM", "NEW YORK": "NY", "NORTH CAROLINA": "NC", "NORTH DAKOTA": "ND", "OHIO": "OH", "OKLAHOMA": "OK", "OREGON": "OR",
// 	"PENNSYLVANIA": "PA", "RHODE ISLAND": "RI", "SOUTH CAROLINA": "SC", "SOUTH DAKOTA": "SD", "TENNESSEE": "TN", "TEXAS": "TX", "UTAH": "UT",
// 	"VERMONT": "VT", "VIRGINIA": "VA", "WASHINGTON": "WA", "WEST VIRGINIA": "WV", "WISCONSIN": "WI", "WYOMING": "WY", "DISTRICT OF COLUMBIA": "DC",
// 	"MARSHALL ISLANDS": "MH", "ARMED FORCES AFRICA": "AE", "ARMED FORCES AMERICAS": "AA", "ARMED FORCES CANADA": "AE", "ARMED FORCES EUROPE": "AE",
// 	"ARMED FORCES MIDDLE EAST": "AE", "ARMED FORCES PACIFIC": "AP",
// }

var DSKindSet = os.Getenv("DSKINDSET")
var DSKindGolden = os.Getenv("DSKINDGOLDEN")
var DSKindFiber = os.Getenv("DSKINDFIBER")

var ctx context.Context

// var zipMap map[string]CityState // intended to be part of address correction
var ps *pubsub.Client
var topic *pubsub.Topic
var topic2 *pubsub.Topic
var expire *pubsub.Topic
var martopic *pubsub.Topic
var ap http.Client
var sb *storage.Client
var msp *redis.Pool
var fs *datastore.Client
var gm *maps.Client

var topicR *pubsub.Topic

var titleYearAttr = ""
var schoolYearAttr = ""
var statusAttr = ""

func init() {
	ctx = context.Background()

	ps, _ = pubsub.NewClient(ctx, os.Getenv("PROJECTID"))
	fs, _ = datastore.NewClient(ctx, os.Getenv("DSPROJECTID"))
	topic = ps.Topic(os.Getenv("PSOUTPUT"))
	martopic = ps.Topic(os.Getenv("PSOUTPUT"))
	expire = ps.Topic(os.Getenv("PSOUTPUT"))
	topicR = ps.Topic(os.Getenv("PSREPORT"))

	// martopic.PublishSettings.DelayThreshold = 1 * time.Second
	// sb, _ := storage.NewClient(ctx)
	// zipMap, _ = readZipMap(ctx, sb, StorageBucket, "data/zip_city_state.json") // intended to be part of address correction
	ap = http.Client{
		Timeout: time.Second * 2, // Maximum of 2 secs
	}
	// is a duplicate of line 117
	// topicR = ps.Topic(os.Getenv("PSREPORT"))
	msp = &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial:        func() (redis.Conn, error) { return redis.Dial("tcp", os.Getenv("MEMSTORE")) },
	}

	gm, _ = maps.NewClient(maps.WithAPIKey("AIzaSyBvLGRj_RzXIPxo58X1XVtrOs1tc7FwABs"))

	log.Printf("init completed, pubsub topic name: %v", topic)
}

func PostProcessPeople(ctx context.Context, m PubSubMessage) error {
	var input Input
	// log.Printf("Received message %+v", string(m.Data))
	if err := json.Unmarshal(m.Data, &input); err != nil {
		log.Fatalf("Unable to unmarshal message %v with error %v", string(m.Data), err)
	}
	//LogDev(fmt.Sprintf("PubSubMessage PostProcessPeople: %s", string(m.Data)))

	var titleValue = ""
	var mprEmail = ""
	var parsedName NameParsed
	var MARCounter int       // keep track of how many MAR we have
	var outputs []PostRecord // this contains all outputs with a type

	LogDev(fmt.Sprintf("people-post for record: %v", input.Signature.RecordID))

	if len(cfName) == 0 {
		cfName = "people-post"
	}

	sort.Slice(input.Columns, func(i, j int) bool {
		return strings.ToLower(input.Columns[i].Name) < strings.ToLower(input.Columns[j].Name)
	})
	LogDev(fmt.Sprintf("input columns output: %v", input.Columns))

	// iterate through every column on the input record to decide what the column is...
	for _, column := range input.Columns {

		column.Value = strings.TrimSpace(column.Value)
		column.Value = reNewline.ReplaceAllString(column.Value, " ") //TODO: Jie this removes carriage return... do we want this here?
		column.Value = strings.Replace(column.Value, "  ", " ", -1)
		column.Value = strings.Replace(column.Value, "  ", " ", -1) //TODO: why are we running this twice in a row?
		if len(column.Value) == 0 {                                 //dont need to work with blank values
			continue
		}

		if column.PeopleERR.SchoolYear == 1 {
			// right now schoolYear is just a pass through value...
			// we do expect to get this w/ most files
			schoolYearAttr = column.Value
		}
		if column.PeopleERR.Status == 1 && column.IsAttribute {
			statusAttr = column.Value
		}
		if (column.PeopleERR.Title == 1 || column.PeopleERR.ContainsTitle == 1) && !reNameTitle.MatchString(column.Value) {
			// !reNameTitle makes sure the value is not like a Mr. Mrs.
			// we don't want to overwrite a file supplied TITLE w/ an attribute...
			if !column.IsAttribute {
				titleValue = column.Value
			} else if column.IsAttribute && titleValue == "" {
				titleValue = column.Value
			}
		}
		if column.PeopleERR.ContainsRole == 1 || column.PeopleERR.FullName == 1 || (column.PeopleVER.IS_FIRSTNAME && column.PeopleVER.IS_LASTNAME && ((column.PeopleERR.ContainsFirstName == 1 && column.PeopleERR.ContainsLastName == 1) || (column.PeopleERR.ContainsFirstName == 0 && column.PeopleERR.ContainsLastName == 0))) {
			parsedName = ParseName(column.Value)
		}

		/**************************************************************/
		// declare column

		/************
		// turned the ML off :(
		// capture ML prediction to column
		// predictionValue := input.Prediction.Predictions[index]
		// predictionKey := strconv.Itoa(int(predictionValue))
		// mlMatchKey := MLLabels[predictionKey]
		// column.MatchKey = mlMatchKey
		*************/

		matchKeyAssigned := columnMatchOverride(column, titleValue, parsedName)

		/****************
		// this was code... that would use a model prediction if rules didn't find something
		// now that we have finished assignment, let's assign the columns, attempting to set value on a match key field that already contains a value will result in additional output being created
		matchKeyAssigned := ""
		if len(column.MatchKey1) > 0 {
			matchKeyAssigned = column.MatchKey1
			LogDev(fmt.Sprintf("matchkey assigned is %v from rules", matchKeyAssigned))
		} else if len(column.MatchKey) > 0 { // use the model default
			matchKeyAssigned = column.MatchKey
			LogDev(fmt.Sprintf("matchkey assigned is %v from prediction", matchKeyAssigned))
		}
		*****/

		/**************************************************************/
		// sanitize values
		if column.MatchKey1 == "ZIP" && IsInt(column.Value) {
			// fix zip code that has leading 0 stripped out
			if len(column.Value) == 8 {
				column.Value = LeftPad2Len(column.Value, "0", 9)
			} else if len(column.Value) == 4 {
				column.Value = LeftPad2Len(column.Value, "0", 5)
			} else if len(column.Value) == 3 { //for PR and the IRS
				column.Value = LeftPad2Len(column.Value, "00", 5)
			}
		}
		if column.MatchKey1 == "EMAIL" && len(column.Value) > 0 {
			// type email if ends with gmail, yahoo, hotmail
			email := strings.ToLower(column.Value)
			if strings.HasSuffix(email, "gmail.com") || strings.HasSuffix(email, "yahoo.com") || strings.HasSuffix(email, "hotmail.com") || strings.HasSuffix(email, "msn.com") || strings.HasSuffix(email, "aol.com") || strings.HasSuffix(email, "comcast.net") {
				column.Type = "Private"
			}
		}
		if column.MatchKey1 == "AD1" && (column.Value != "true" && column.Value != "false" && column.Value != "0001-01-01T00:00:00+00:00" && column.Value != "0.000000") {
			//Ad Type
			column.Type = AssignAddressType(&column)
			column.MatchKey2 = "ADTYPE"
			column.MatchKey3 = "ADBOOK"
		}

		var currentOutput *PostRecord
		var indexOutput int
		skipValue := false

		if len(matchKeyAssigned) > 0 { // if we haven't assigned a value to the column...
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
			} else if column.PeopleERR.ContainsRole == 1 { // MPR
				if column.PeopleERR.ContainsMother == 1 {
					currentOutput, indexOutput = GetOutputByTypeAndSequence(&outputs, "mpr", 1)
				} else if column.PeopleERR.ContainsFather == 1 {
					currentOutput, indexOutput = GetOutputByTypeAndSequence(&outputs, "mpr", 2)
				} else {
					if column.PeopleVER.IS_EMAIL {
						// could use an iterator in here instead of 3 && 4
						if mprEmail != column.Value {
							currentOutput, indexOutput = GetOutputByTypeAndSequence(&outputs, "mpr", 3)
							mprEmail = column.Value
						} else {
							currentOutput, indexOutput = GetOutputByTypeAndSequence(&outputs, "mpr", 4)
						}
					} else {
						currentOutput, indexOutput = GetOutputByTypeAndSequence(&outputs, "mpr", 3)
					}
				}
			}

			// if column.PeopleERR.ContainsMother == 1 {
			// 	currentOutput, indexOutput = GetOutputByTypeAndSequence(&outputs, "mpr", 1)
			// } else if column.PeopleERR.ContainsFather == 1 {
			// 	currentOutput, indexOutput = GetOutputByTypeAndSequence(&outputs, "mpr", 2)
			// } else if column.PeopleVER.IS_EMAIL { //handle case where we get two "parent" emails
			// 	if mprEmail != column.Value {
			// 		currentOutput, indexOutput = GetOutputByTypeAndSequence(&outputs, "mpr", 4)
			// 		mprEmail = column.Value
			// 	} else {
			// 		currentOutput, indexOutput = GetOutputByTypeAndSequence(&outputs, "mpr", 5)
			// 	}
			// } else {
			// 	currentOutput, indexOutput = GetOutputByTypeAndSequence(&outputs, "mpr", 3)
			// }

			if !skipValue {
				// let's assign the value
				switch matchKeyAssigned {
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

				if len(column.MatchKey2) > 0 && column.MatchKey2 == "ADTYPE" {
					SetMkField(&(currentOutput.Output), "ADTYPE", AssignAddressType(&column), column.Name)
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

		// if we have a TITLE, lets standardize it.
		if len(v.Output.TITLE.Value) > 0 {
			v.Output.TITLE.Value, v.Output.STATUS.Value = CalcClassYear(v.Output.TITLE.Value, schoolYearAttr, statusAttr, true)
		}

		// let's populate city state if we have zip
		// if len(v.Output.ZIP.Value) >= 5 && (v.Output.COUNTRY.Value == "US" || v.Output.COUNTRY.Value == "USA" || strings.ToLower(v.Output.COUNTRY.Value) == "united states of america" || strings.ToLower(v.Output.COUNTRY.Value) == "united states" || v.Output.COUNTRY.Value == "") && v.Type != "mar" {
		// 	if len(v.Output.CITY.Value) == 0 && len(v.Output.STATE.Value) == 0 {
		// 		v.Output.CITY.Value, v.Output.STATE.Value = populateCityStateFromZip(v.Output.ZIP.Value)
		// 		if len(v.Output.CITY.Value) > 0 || len(v.Output.STATE.Value) > 0 {
		// 			v.Output.CITY.Source = "WM"
		// 			v.Output.STATE.Source = "WM"
		// 		}
		// 		LogDev(fmt.Sprintf("v.Output.STATE.Value: %v, v.Output.STATE.Source: %v, v.Output.CITY.Value: %v, v.Output.CITY.Source: %v ", v.Output.STATE.Value, v.Output.STATE.Source, v.Output.CITY.Value, v.Output.CITY.Source))

		// 	} else if len(v.Output.STATE.Value) == 0 {
		// 		_, v.Output.STATE.Value = populateCityStateFromZip(v.Output.ZIP.Value)
		// 		if len(v.Output.STATE.Value) > 0 {
		// 			v.Output.STATE.Source = "WM"
		// 		}
		// 		LogDev(fmt.Sprintf("v.Output.STATE.Value: %v, v.Output.STATE.Source: %v", v.Output.STATE.Value, v.Output.STATE.Source))
		// 	} else if len(v.Output.CITY.Value) == 0 {
		// 		v.Output.CITY.Value, _ = populateCityStateFromZip(v.Output.ZIP.Value)
		// 		if len(v.Output.CITY.Value) > 0 {
		// 			v.Output.CITY.Source = "WM"
		// 		}
		// 		LogDev(fmt.Sprintf("v.Output.CITY.Value: %v, v.Output.CITY.Source: %v", v.Output.CITY.Value, v.Output.CITY.Source))
		// 	}
		// }

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
		if reState.MatchString(v.Output.STATE.Value) && (v.Output.COUNTRY.Value == "US" || v.Output.COUNTRY.Value == "USA" || strings.ToLower(v.Output.COUNTRY.Value) == "united states of america" || strings.ToLower(v.Output.COUNTRY.Value) == "united states" || v.Output.COUNTRY.Value == "") {
			LogDev(fmt.Sprintf("overriding country by state value: %v", v.Output.STATE.Value))
			v.Output.COUNTRY.Value = "US"
			v.Output.COUNTRY.Source = "WM"
		}

		// if ad1 and ad2 are equals, drop ad2
		if v.Output.AD1.Value == v.Output.AD2.Value {
			v.Output.AD2.Value = ""
			v.Output.AD2.Source = ""
		}

		// swap ad1 and ad2 if ad2 is not blank but ad1 is
		if len(v.Output.AD1.Value) == 0 && len(v.Output.AD2.Value) > 0 {
			v.Output.AD1.Value = v.Output.AD2.Value
			v.Output.AD1.Source = v.Output.AD2.Source
			v.Output.AD2.Value = ""
			v.Output.AD2.Source = ""
		}

		// If we could not identify another country previously...
		// including US... meaning we don't have a state
		// yet we have an address
		if v.Output.AD1.Value != "" && v.Output.COUNTRY.Value == "" && v.Type != "mar" {
			LogDev(fmt.Sprintf("trying to find a country %v %v %v", v.Output.AD1.Value, v.Output.AD2.Value, v.Output.STATE.Value))
			if v.Output.STATE.Value == "other" {
				v.Output.COUNTRY.Value = "INTL"
			}
			// TODO: Jie can you look at this poop...
			// scan Ad1, Ad2, Ad3, Ad4... to see if we can find a country code...
		}

		// IF we believe it to NOT be an international address...
		// if v.Output.COUNTRY.Value == "US" || v.Output.COUNTRY.Value == "USA" || v.Output.COUNTRY.Value == "United States" || v.Output.COUNTRY.Value == "United States of America" || v.Output.COUNTRY.Value == "America" {
		if v.Output.COUNTRY.Value == "US" || v.Output.COUNTRY.Value == "" && v.Type != "mar" {
			v.Output.ADVALID.Value = "FALSE"
			v.Output.ADCORRECT.Value = "FALSE"
			StandardizeAddressSmartyStreet(&(v.Output))
			// StandardizeAddressLP(&(v.Output)) // not using libpostal right now...
			if v.Output.ADVALID.Value == "TRUE" {

			} else { // try google
				StandardizeAddressGoogleMap(&(v.Output))
			}
		}

		// try to stick a value into STATE if it is blank and we believe it is international
		if len(v.Output.CITY.Value) > 0 && len(v.Output.STATE.Value) == 0 && !reZip5.MatchString(v.Output.ZIP.Value) && !reZip9.MatchString(v.Output.ZIP.Value) && v.Type != "mar" {
			v.Output.STATE.Source = "WM"
			v.Output.STATE.Value = "UNKNOWN"
		} else if len(v.Output.CITY.Value) > 0 && len(v.Output.STATE.Value) == 0 && len(v.Output.COUNTRY.Value) == 0 && v.Type != "mar" {
			v.Output.STATE.Source = "WM"
			v.Output.STATE.Value = "UNKNOWN"
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

		// preload Set (Search:[FiberID])
		if len(input.Signature.RecordID) == 0 {
			// ensure record id is not blank or we'll have problem
			input.Signature.RecordID = uuid.New().String()
		}
		var searchFields []string
		reportCounters := []ReportCounter{}
		searchFields = append(searchFields, fmt.Sprintf("RECORDID=%v", input.Signature.RecordID))
		searchFields = append(searchFields, fmt.Sprintf("HOUSE=&RECORDID=%v", input.Signature.RecordID))

		if len(v.Output.EMAIL.Value) > 0 {
			searchFields = append(searchFields, fmt.Sprintf("EMAIL=%v&ROLE=%v", strings.TrimSpace(strings.ToUpper(v.Output.EMAIL.Value)), strings.TrimSpace(strings.ToUpper(v.Output.ROLE.Value)))) // for people
			searchFields = append(searchFields, fmt.Sprintf("HOUSE=&EMAIL=%v", strings.TrimSpace(strings.ToUpper(v.Output.EMAIL.Value))))                                                           // for house
			// reportCounters = append(reportCounters, ReportCounter{Type: "PeoplePost", Name: "People:Email", Count: 1, Increment: true})
		}
		if len(v.Output.PHONE.Value) > 0 && len(v.Output.FINITIAL.Value) > 0 {
			searchFields = append(searchFields, fmt.Sprintf("PHONE=%v&FINITIAL=%v&ROLE=%v", strings.TrimSpace(strings.ToUpper(v.Output.PHONE.Value)), strings.TrimSpace(strings.ToUpper(v.Output.FINITIAL.Value)), strings.TrimSpace(strings.ToUpper(v.Output.ROLE.Value))))
			// reportCounters = append(reportCounters, ReportCounter{Type: "PeoplePost", Name: "People:Phone+FInitial", Count: 1, Increment: true})
		}

		if len(v.Output.CITY.Value) > 0 && len(v.Output.STATE.Value) > 0 && len(v.Output.LNAME.Value) > 0 && len(v.Output.FNAME.Value) > 0 && len(v.Output.AD1.Value) > 0 {
			searchFields = append(searchFields, fmt.Sprintf("FNAME=%v&LNAME=%v&AD1=%v&CITY=%v&STATE=%v&ROLE=%v", strings.TrimSpace(strings.ToUpper(v.Output.FNAME.Value)), strings.TrimSpace(strings.ToUpper(v.Output.LNAME.Value)), strings.TrimSpace(strings.ToUpper(v.Output.AD1.Value)), strings.TrimSpace(strings.ToUpper(v.Output.CITY.Value)), strings.TrimSpace(strings.ToUpper(v.Output.STATE.Value)), strings.TrimSpace(strings.ToUpper(v.Output.ROLE.Value))))
			// reportCounters = append(reportCounters, ReportCounter{Type: "PeoplePost", Name: "People:City+State+LName+FName+AD1", Count: 1, Increment: true})
		}
		// for house
		if len(v.Output.CITY.Value) > 0 && len(v.Output.STATE.Value) > 0 && len(v.Output.AD1.Value) > 0 {
			if len(v.Output.AD2.Value) > 0 {
				searchFields = append(searchFields, fmt.Sprintf("HOUSE=&AD1=%v&AD2=%v&CITY=%v&STATE=%v", strings.TrimSpace(strings.ToUpper(v.Output.AD1.Value)), strings.TrimSpace(strings.ToUpper(v.Output.AD2.Value)), strings.TrimSpace(strings.ToUpper(v.Output.CITY.Value)), strings.TrimSpace(strings.ToUpper(v.Output.STATE.Value))))
			} else {
				searchFields = append(searchFields, fmt.Sprintf("HOUSE=&AD1=%v&CITY=%v&STATE=%v", strings.TrimSpace(strings.ToUpper(v.Output.AD1.Value)), strings.TrimSpace(strings.ToUpper(v.Output.CITY.Value)), strings.TrimSpace(strings.ToUpper(v.Output.STATE.Value))))
			}
		}

		dsNameSpace := strings.ToLower(fmt.Sprintf("%v-%v", env, input.Signature.OwnerID))
		log.Printf("Searchfields %+v", searchFields)
		if len(searchFields) > 0 {
			for _, search := range searchFields {
				fiberRedisKey := []string{input.Signature.OwnerID, "search-fibers", search} // existing fibers
				setRedisKey := []string{input.Signature.OwnerID, "search-sets", search}     // existing sets
				searchValue := strings.Replace(search, "'", `''`, -1)
				querySets := []PeopleSetDS{}
				if _, err := fs.GetAll(ctx, datastore.NewQuery(DSKindSet).Namespace(dsNameSpace).Filter("search =", searchValue), &querySets); err != nil {
					log.Fatalf("Error querying sets for %v: %v", searchValue, err)
				}
				log.Printf("Fiber type %v Search %v found %v sets", v.Type, search, len(querySets))
				for _, s := range querySets {
					if len(s.Fibers) > 0 {
						for _, f := range s.Fibers {
							AppendRedisTempKey(fiberRedisKey, f)
							// log.Printf("fiberRedisKey %v f %v ", fiberRedisKey, f)
						}
					}
					AppendRedisTempKey(setRedisKey, s.ID.Name)
					// log.Printf("setRedisKey %v s.ID.Name %v ", setRedisKey, s.ID.Name)
				}
				// get the golden records
				var goldenKeys []*datastore.Key
				var goldenIDs []string
				var goldens []PeopleGoldenDS
				for _, setKey := range querySets {
					if !Contains(goldenIDs, setKey.ID.Name) {
						goldenIDs = append(goldenIDs, setKey.ID.Name)
						dsGoldenGetKey := datastore.NameKey(DSKindGolden, setKey.ID.Name, nil)
						dsGoldenGetKey.Namespace = dsNameSpace
						goldenKeys = append(goldenKeys, dsGoldenGetKey)
						goldens = append(goldens, PeopleGoldenDS{})
					}
				}
				if len(goldenKeys) > 0 {
					batchSize := 1000
					l := len(goldenKeys) / batchSize

					if len(goldenKeys)%batchSize > 0 {
						l++
					}
					for r := 0; r < l; r++ {
						s := r * 1000
						e := s + 1000

						if e > len(goldenKeys) {
							e = len(goldenKeys)
						}

						gk := goldenKeys[s:e]
						gd := goldens[s:e]

						if err := fs.GetMulti(ctx, gk, gd); err != nil && err != datastore.ErrNoSuchEntity {
							log.Printf("Error fetching golden records ns %v kind %v, key count %v: %v,", dsNameSpace, DSKindGolden, len(goldenKeys), err)
						}

					}
				}
				for _, g := range goldens {
					// if g.ROLE != "Parent" {
					// Here we can receive an empty golden if we had the error "fetching golden error" line 616
					if g.ID != nil  && g.ROLE != "Parent" {
						SetRedisTempKeyWithValue([]string{input.Signature.OwnerID, g.ID.Name, "golden", "title"}, g.TITLE)
						if g.ADVALID == "TRUE" {
							SetRedisKeyWithExpiration([]string{input.Signature.OwnerID, g.ID.Name, "golden", "advalid"})
						} else {
							SetRedisKeyWithExpiration([]string{input.Signature.OwnerID, g.ID.Name, "golden", "nonadvalid"})
						}
						if g.COUNTRY != "US" {
							SetRedisKeyWithExpiration([]string{input.Signature.OwnerID, g.ID.Name, "golden", "international"})
						}
						if len(g.EMAIL) > 0 {
							SetRedisKeyWithExpiration([]string{input.Signature.OwnerID, g.ID.Name, "golden", "email"})
						}
					}
				}
			}
		}

		pubQueue = append(pubQueue, PubQueue{
			Output: v.Output,
			Suffix: suffix,
			Type:   v.Type,
		})

		matchKeyStat := map[string]int{}
		if v.Type == "default" {
			if len(v.Output.AD1.Value) > 0 {
				matchKeyStat["AD1"] = 1
			}
			if len(v.Output.AD2.Value) > 0 {
				matchKeyStat["AD2"] = 1
			}
			if len(v.Output.FNAME.Value) > 0 {
				matchKeyStat["FNAME"] = 1
			}
			if len(v.Output.LNAME.Value) > 0 {
				matchKeyStat["LNAME"] = 1
			}
			if len(v.Output.CITY.Value) > 0 {
				matchKeyStat["CITY"] = 1
			}
			if len(v.Output.STATE.Value) > 0 {
				matchKeyStat["STATE"] = 1
			}
			if len(v.Output.ZIP.Value) > 0 {
				matchKeyStat["ZIP"] = 1
			}
			if len(v.Output.EMAIL.Value) > 0 {
				matchKeyStat["EMAIL"] = 1
			}
			if len(v.Output.PHONE.Value) > 0 {
				matchKeyStat["PHONE"] = 1
			}
			if len(v.Output.COUNTRY.Value) > 0 {
				matchKeyStat["COUNTRY"] = 1
			}
		}

		reportCounters = append(reportCounters, ReportCounter{Type: "PeoplePost", Name: v.Type, Count: 1, Increment: true})
		reportCounters = append(reportCounters, ReportCounter{Type: "PeoplePost", Name: "Total", Count: 1, Increment: true})
		report := FileReport{
			ID:                 input.Signature.EventID,
			Counters:           reportCounters,
			MatchKeyStatistics: matchKeyStat,
		}
		publishReport(&report, cfName)
	}

	var pubs []Output
	var reportFibers []FiberDetail
	var recordFibers []string
	for _, p := range pubQueue {
		var output Output
		output.Signature = input.Signature
		output.Signature.FiberType = p.Type
		if len(p.Suffix) > 0 {
			output.Signature.RecordID += p.Suffix
		}
		output.Passthrough = input.Passthrough
		output.MatchKeys = p.Output
		output.Signature.FiberID = uuid.New().String()
		pubs = append(pubs, output)

		recordFibers = append(recordFibers, output.Signature.FiberID)
		reportFibers = append(reportFibers, FiberDetail{
			ID:        output.Signature.FiberID,
			CreatedOn: time.Now(),
			Type:      output.Signature.FiberType,
		})

		// write the mapping to report
		matchKeyNames := structs.Names(&PeopleOutput{})
		mappingResult := []NameValue{}
		for _, mkn := range matchKeyNames {
			mkfield := GetMkField(&(p.Output), mkn)
			if len(mkfield.Source) > 0 && mkfield.Source != "WM" {
				mappingResult = append(mappingResult, NameValue{
					Name:  mkfield.Source,
					Value: mkn,
				})
			}
		}
		report := FileReport{
			ID:         input.Signature.EventID,
			ColumnMaps: mappingResult,
		}
		publishReport(&report, cfName)
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
	// _, err = psresult.Get(ctx)
	if err != nil {
		log.Fatalf("%v Could not pub to pubsub error: %v: output: %v", input.Signature.EventID, err, string(outputJSON))
	} else {
		log.Printf("%v pubbed record as message id %v: %v", input.Signature.EventID, psid, string(outputJSON))
		report := FileReport{
			ID: input.Signature.EventID,
			RecordList: []RecordDetail{
				RecordDetail{
					ID:     input.Signature.RecordID,
					Fibers: recordFibers,
				},
			},
			FiberList: reportFibers,
		}
		publishReport(&report, cfName)
	}
	return nil
}
