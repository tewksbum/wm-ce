// Package addressprocessor contains a series of cloud functions for streamer
package addressprocessor

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	"cloud.google.com/go/pubsub"
	"googlemaps.github.io/maps"
	"github.com/gomodule/redigo/redis"
	"cloud.google.com/go/datastore"
)

// Customer contains Customer fields

type Input struct {
	NetsuiteKey string `json:"netsuite_key"`
	Owner       string `json:"owner"`
	EventID     string`json:"event_id"`
	EventType   string`json:"event_type"`
	AddressType   string`json:"address_type"`
	Source      string `json:"source"`
	Name      string `json:"name"`
	Addr1      string `json:"addr1"`
	Addr2      string `json:"addr2"`
	City      string `json:"city"`
	State      string `json:"state"`
	Zip      string `json:"zip"`
	Country      string `json:"country"`
	Phone      string `json:"phone"`
	Email      string `json:"email"`
}


// ProjectID is the env var of project id
var ProjectID = os.Getenv("PROJECTID")
// DSProjectID is the env var of project id
var DSProjectID = os.Getenv("DSPROJECTID")
var PubSubTopic = os.Getenv("PSOUTPUT")
var SmartyStreetsEndpoint = os.Getenv("SMARTYSTREET")

var reNewline = regexp.MustCompile(`\r?\n`)
var reFullName = regexp.MustCompile(`^(.+?) ([^\s,]+)(,? (?:[JS]r\.?|III?|IV))?$`)
var reFullName2 = regexp.MustCompile(`^(.*), (.*) (.{1})\.$`) // Wilson, Lauren K.
var reFullName3 = regexp.MustCompile(`^(.*), (.*)$`)          // Wilson, Lauren K.
var reFullName4 = regexp.MustCompile(`^(.*),(.*)$`)           // Wilson,Lauren
var reFullName5 = regexp.MustCompile(`^(.*),(.*)( .{1}\.)$`)  //// Wilson,Lauren K.
var reState = regexp.MustCompile(`(?i)^(AL|AK|AZ|AR|CA|CO|CT|DC|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|PR|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY)$`)
var reStateFull = regexp.MustCompile(`(?i)^(alabama|alaska|arizona|arkansas|california|colorado|connecticut|delaware|district of columbia|florida|georgia|hawaii|idaho|illinois|indiana|iowa|kansas|kentucky|louisiana|maine|maryland|massachusetts|michigan|minnesota|mississippi|missouri|montana|nebraska|nevada|new hampshire|new jersey|new mexico|new york|north carolina|north dakota|ohio|oklahoma|oregon|pennsylvania|rhode island|south carolina|south dakota|tennessee|texas|utah|vermont|virginia|washington|west virginia|wisconsin|wyoming)$`)
var redisTemporaryExpiration = 3600
var DSKindSet = os.Getenv("DSKINDSET")
var env = os.Getenv("ENVIRONMENT")
var dev = (env == "dev")

// global vars
var ctx context.Context
var ps *pubsub.Client
var topic *pubsub.Topic
var ap http.Client
var gm *maps.Client
var msp *redis.Pool
var fs *datastore.Client

func init() {
	ctx = context.Background()
	ps, _ = pubsub.NewClient(ctx, ProjectID)
	topic = ps.Topic(PubSubTopic)
	msp = &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial:        func() (redis.Conn, error) { return redis.Dial("tcp", os.Getenv("MEMSTORE")) },
	}
	gm, _ = maps.NewClient(maps.WithAPIKey("AIzaSyBvLGRj_RzXIPxo58X1XVtrOs1tc7FwABs"))
	fs, _ = datastore.NewClient(ctx, os.Getenv("DSPROJECTID"))
	log.Printf("init completed, pubsub topic names: %v", topic)
}

// ProcessAddress Receives a http event request
func ProcessAddress(ctx context.Context, m PubSubMessage) error {
	var input Input
	LogDev(fmt.Sprintf("input: %v", string(m.Data)))
	if err := json.Unmarshal(m.Data, &input); err != nil {
		log.Fatalf("Error: Unable to unmarshal message %v with error %v", string(m.Data), err)
	}

	if len(input.NetsuiteKey) == 0 {
		input.NetsuiteKey = input.EventID
	}

	if len(input.Owner) == 0 {
		input.Owner = "ocm"
	}

	if input.Phone == "2222222222" || input.Phone == "5555555555" {
		input.Phone = ""
	}

	input.NetsuiteKey = fmt.Sprintf("%-36v", input.NetsuiteKey) // make these IDs 36 character long because 360 assumes these are GUID
	
	var output Output
	output.Signature = Signature{
		OwnerID:   input.Owner,
		Source:    input.Source,
		EventID:   input.EventID,
		EventType: input.EventType,
		FiberType: "default",
		FiberID: input.NetsuiteKey,
		RecordID: input.NetsuiteKey,
	}

	
	var person PeopleOutput
	person.ADBOOK.Value = input.AddressType
	names := ParseName(input.Name)
	person.FNAME.Value = names.FNAME
	person.LNAME.Value = names.LNAME
	person.AD1.Value = input.Addr1
	person.AD2.Value = input.Addr2
	person.CITY.Value = input.City
	person.STATE.Value = input.State
	person.ZIP.Value = input.Zip
	person.COUNTRY.Value = input.Country
	if person.COUNTRY.Value == "" {
		person.COUNTRY.Value = "US"
	}
	person.EMAIL.Value = input.Email
	person.PHONE.Value = input.Phone
	if len(input.NetsuiteKey) > 0 {
		person.CLIENTID.Value = "NETSUITE." + input.NetsuiteKey
	}

	person.ADVALID.Value = "FALSE"
	person.ADCORRECT.Value = "FALSE"

	// perform some clean up
	if reStateFull.MatchString(strings.ToLower(person.STATE.Value)) {
		LogDev(fmt.Sprintf("standardizing STATE to 2 letter abbreviation: %v", person.STATE.Value))
		person.STATE.Value = lookupState(person.STATE.Value)
	}
	// standardize "US" for any US State address
	if reState.MatchString(person.STATE.Value) && (person.COUNTRY.Value == "US" || person.COUNTRY.Value == "USA" || strings.ToLower(person.COUNTRY.Value) == "united states of america" || strings.ToLower(person.COUNTRY.Value) == "united states" || person.COUNTRY.Value == "") {
		LogDev(fmt.Sprintf("overriding country by state value: %v", person.STATE.Value))
		person.COUNTRY.Value = "US"
		person.COUNTRY.Source = "WM"
	}

	// if ad1 and ad2 are equals, drop ad2
	if person.AD1.Value == person.AD2.Value {
		person.AD2.Value = ""
		person.AD2.Source = ""
	}

	// swap ad1 and ad2 if ad2 is not blank but ad1 is
	if len(person.AD1.Value) == 0 && len(person.AD2.Value) > 0 {
		person.AD1.Value = person.AD2.Value
		person.AD1.Source = person.AD2.Source
		person.AD2.Value = ""
		person.AD2.Source = ""
	}

	StandardizeAddressSmartyStreet(&person)
	if person.ADVALID.Value != "TRUE" {
		StandardizeAddressGoogleMap(&person)
	}
	output.MatchKeys = person
	
	otherValues := make(map[string]string)
	otherValues["netsuite_key"] = input.NetsuiteKey
	output.Passthrough = otherValues

	// compute search fields and pre-load into redis
	var searchFields []string
	searchFields = append(searchFields, fmt.Sprintf("RECORDID=%v", input.NetsuiteKey))
	searchFields = append(searchFields, fmt.Sprintf("HOUSE=&RECORDID=%v", input.NetsuiteKey))

	if len(person.EMAIL.Value) > 0 {
		searchFields = append(searchFields, fmt.Sprintf("EMAIL=%v&ROLE=%v", strings.TrimSpace(strings.ToUpper(person.EMAIL.Value)), strings.TrimSpace(strings.ToUpper(person.ROLE.Value)))) // for people
		searchFields = append(searchFields, fmt.Sprintf("HOUSE=&EMAIL=%v", strings.TrimSpace(strings.ToUpper(person.EMAIL.Value))))                                                           // for house
	}
	if len(person.PHONE.Value) > 0 && len(person.FINITIAL.Value) > 0 {
		searchFields = append(searchFields, fmt.Sprintf("PHONE=%v&FINITIAL=%v&ROLE=%v", strings.TrimSpace(strings.ToUpper(person.PHONE.Value)), strings.TrimSpace(strings.ToUpper(person.FINITIAL.Value)), strings.TrimSpace(strings.ToUpper(person.ROLE.Value))))
	}

	if len(person.CITY.Value) > 0 && len(person.STATE.Value) > 0 && len(person.LNAME.Value) > 0 && len(person.FNAME.Value) > 0 && len(person.AD1.Value) > 0 {
		searchFields = append(searchFields, fmt.Sprintf("FNAME=%v&LNAME=%v&AD1=%v&CITY=%v&STATE=%v&ROLE=%v", strings.TrimSpace(strings.ToUpper(person.FNAME.Value)), strings.TrimSpace(strings.ToUpper(person.LNAME.Value)), strings.TrimSpace(strings.ToUpper(person.AD1.Value)), strings.TrimSpace(strings.ToUpper(person.CITY.Value)), strings.TrimSpace(strings.ToUpper(person.STATE.Value)), strings.TrimSpace(strings.ToUpper(person.ROLE.Value))))
	}
	// for house
	if len(person.CITY.Value) > 0 && len(person.STATE.Value) > 0 && len(person.AD1.Value) > 0 {
		if len(person.AD2.Value) > 0 {
			searchFields = append(searchFields, fmt.Sprintf("HOUSE=&AD1=%v&AD2=%v&CITY=%v&STATE=%v", strings.TrimSpace(strings.ToUpper(person.AD1.Value)), strings.TrimSpace(strings.ToUpper(person.AD2.Value)), strings.TrimSpace(strings.ToUpper(person.CITY.Value)), strings.TrimSpace(strings.ToUpper(person.STATE.Value))))
		} else {
			searchFields = append(searchFields, fmt.Sprintf("HOUSE=&AD1=%v&CITY=%v&STATE=%v", strings.TrimSpace(strings.ToUpper(person.AD1.Value)), strings.TrimSpace(strings.ToUpper(person.CITY.Value)), strings.TrimSpace(strings.ToUpper(person.STATE.Value))))
		}
	}

	dsNameSpace := strings.ToLower(fmt.Sprintf("%v-%v", env, output.Signature.OwnerID))

	if len(searchFields) > 0 {
		for _, search := range searchFields {
			fiberRedisKey := []string{output.Signature.OwnerID, "search-fibers", search} // existing fibers
			setRedisKey := []string{output.Signature.OwnerID, "search-sets", search}     // existing sets
			searchValue := strings.Replace(search, "'", `''`, -1)
			querySets := []PeopleSetDS{}
			if _, err := fs.GetAll(ctx, datastore.NewQuery(DSKindSet).Namespace(dsNameSpace).Filter("search =", searchValue), &querySets); err != nil {
				log.Fatalf("Error querying sets: %v", err)
			}
			log.Printf("Fiber search %v found %v sets", search, len(querySets))
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
		}
	}

	outputJSON, _ := json.Marshal([]Output{output})
	// this is a data request, drop to eventdata pubsub
	psresult := topic.Publish(ctx, &pubsub.Message{
		Data: outputJSON,
	})

	psid, err := psresult.Get(ctx)
	_, err = psresult.Get(ctx)
	if err != nil {
		log.Fatalf("%v Could not pub to pubsub: %v", output.Signature.EventID, err)
	} else {
		log.Printf("%v pubbed record as message id %v: %v", output.Signature.EventID, psid, string(outputJSON))
	}

	return nil
}

func ParseName(v string) NameParsed {
	result := reFullName.FindStringSubmatch(strings.Replace(v, "&", ",", -1))
	if len(result) >= 3 {
		// ignore 0
		fname := result[1]
		lname := result[2]
		suffix := result[3]
		if strings.HasSuffix(fname, ",") || strings.HasSuffix(lname, ".") || strings.Contains(fname, ",") {
			parsed1 := reFullName2.FindStringSubmatch(v)
			if len(parsed1) >= 3 {
				lname = parsed1[1]
				fname = parsed1[2]
				suffix = ""
			} else {
				parsed2 := reFullName3.FindStringSubmatch(v)
				if len(parsed2) >= 2 {
					lname = parsed2[1]
					fname = parsed2[2]
					suffix = ""
				} else {
					parsed3 := reFullName5.FindStringSubmatch(v)
					if len(parsed3) >= 2 {
						lname = parsed3[1]
						fname = parsed3[2]
						suffix = ""
					}
				}
			}
		}
		return NameParsed{
			FNAME:  fname,
			LNAME:  lname,
			SUFFIX: suffix,
		}
	}
	result = reFullName4.FindStringSubmatch(v)
	if len(result) >= 2 {
		lname := result[1]
		fname := result[2]
		suffix := ""
		return NameParsed{
			FNAME:  fname,
			LNAME:  lname,
			SUFFIX: suffix,
		}
	}
	return NameParsed{}
}

func StandardizeAddressGoogleMap(mkOutput *PeopleOutput) {
	addressInput := mkOutput.AD1.Value + ", " + mkOutput.AD2.Value + ", " + mkOutput.CITY.Value + ", " + mkOutput.STATE.Value + " " + mkOutput.ZIP.Value + ", " + mkOutput.COUNTRY.Value
	if mkOutput.COUNTRY.Value == "US" {
		addressInput = mkOutput.AD1.Value + ", " + mkOutput.AD2.Value + ", " + mkOutput.CITY.Value + ", " + mkOutput.STATE.Value + " " + mkOutput.ZIP.Value
		if len(strings.TrimSpace(addressInput)) > 10 {
			gmResult, err := gm.Geocode(ctx, &maps.GeocodingRequest{
				Address: addressInput,
			})
			if err != nil {
				log.Printf("Google Maps error %v", err)
			}

			if len(gmResult) > 0 && len(gmResult[0].FormattedAddress) > 0 {
				// pick the first result
				log.Printf("Google Maps returned %v", gmResult[0].FormattedAddress)
				streetNumber := ""
				streetName := ""
				unitNumber := ""
				unitType := ""
				city := ""
				state := ""
				zip := ""
				zip4 := ""
				country := ""
				for _, component := range gmResult[0].AddressComponents {
					if Contains(component.Types, "street_number") {
						streetNumber = component.ShortName
					} else if Contains(component.Types, "route") {
						streetName = component.ShortName
					} else if Contains(component.Types, "locality") {
						city = component.ShortName
					} else if Contains(component.Types, "administrative_area_level_1") {
						state = component.ShortName
					} else if Contains(component.Types, "country") {
						country = component.ShortName
					} else if Contains(component.Types, "postal_code") {
						zip = component.ShortName
					} else if Contains(component.Types, "postal_code_suffix") {
						zip4 = component.ShortName
					} else if Contains(component.Types, "subpremise") {
						unitNumber = component.ShortName
					}

				}
				mkOutput.ADCORRECT.Value = "TRUE"
				mkOutput.AD1.Value = streetNumber + " " + streetName
				mkOutput.AD1NO.Value = streetNumber
				if len(unitNumber) > 0 && len(unitType) == 0 {

				}
				mkOutput.AD2.Value = strings.TrimSpace(unitType + " " + unitNumber)
				mkOutput.CITY.Value = city
				mkOutput.STATE.Value = state
				mkOutput.ZIP.Value = zip
				if len(zip) > 0 && len(zip4) > 0 {
					mkOutput.ZIP.Value = zip + "-" + zip4
				}
				mkOutput.COUNTRY.Value = country
				mkOutput.ADPARSER.Value = "googlemap"
				mkOutput.ADPARSER.Source = "GM"
				mkOutput.ADVALID.Value = "TRUE"
			}

		}
	}

}

func StandardizeAddressSmartyStreet(mkOutput *PeopleOutput) {
	addressInput := mkOutput.AD1.Value + ", " + mkOutput.AD2.Value + ", " + mkOutput.CITY.Value + ", " + mkOutput.STATE.Value + " " + mkOutput.ZIP.Value + ", " + mkOutput.COUNTRY.Value
	if mkOutput.COUNTRY.Value == "US" {
		addressInput = mkOutput.AD1.Value + ", " + mkOutput.AD2.Value + ", " + mkOutput.CITY.Value + ", " + mkOutput.STATE.Value + " " + mkOutput.ZIP.Value
	}
	LogDev(fmt.Sprintf("addressInput passed TO parser %v", addressInput))
	if len(strings.TrimSpace(addressInput)) > 10 {
		a := CorrectAddress(reNewline.ReplaceAllString(addressInput, ""))
		LogDev(fmt.Sprintf("address parser returned %v from input %v", a, addressInput))
		if len(a) > 0 && len(a[0].DeliveryLine1) > 1 { // take the first
			if mkOutput.AD1.Value != a[0].DeliveryLine1 {
				mkOutput.ADCORRECT.Value = "TRUE"
			}
			mkOutput.AD1.Value = strings.TrimSpace(a[0].DeliveryLine1)
			mkOutput.AD1NO.Value = strings.TrimSpace(a[0].Components.PrimaryNumber)
			if len(a[0].Components.SecondaryDesignator) > 0 && len(a[0].Components.SecondaryNumber) > 0 {
				mkOutput.AD2.Value = strings.TrimSpace(a[0].Components.SecondaryDesignator + " " + a[0].Components.SecondaryNumber)
				if strings.HasSuffix(mkOutput.AD1.Value, mkOutput.AD2.Value) {
					mkOutput.AD1.Value = strings.TrimSpace(strings.TrimSuffix(mkOutput.AD1.Value, mkOutput.AD2.Value))
				}
			}
			if mkOutput.CITY.Value != a[0].Components.CityName {
				mkOutput.ADCORRECT.Value = "TRUE"
			}
			mkOutput.CITY.Value = a[0].Components.CityName
			if mkOutput.STATE.Value != a[0].Components.StateAbbreviation {
				mkOutput.ADCORRECT.Value = "TRUE"
			}
			mkOutput.STATE.Value = a[0].Components.StateAbbreviation

			Zip := a[0].Components.Zipcode
			if len(a[0].Components.Plus4Code) > 0 {
				Zip += "-" + a[0].Components.Plus4Code
			}
			mkOutput.ZIP.Value = strings.TrimSpace(Zip)
			mkOutput.COUNTRY.Value = "US"         
			mkOutput.ADPARSER.Value = "smartystreet" 
			mkOutput.ADPARSER.Source = "SS" 
			mkOutput.ADTYPE.Value = strings.TrimSpace(a[0].Metadata.Rdi)
			mkOutput.ZIPTYPE.Value = strings.TrimSpace(a[0].Metadata.ZipType)
			mkOutput.RECORDTYPE.Value = strings.TrimSpace(a[0].Metadata.RecordType)
			mkOutput.ADVALID.Value = "TRUE"
		}
	}

	// pre-empted before StandardizeAddressSS is called...
	//
	// if reState.MatchString(mkOutput.STATE.Value) {
	// 	LogDev(fmt.Sprintf("overriding country by state value: %v", mkOutput.STATE.Value))
	// 	mkOutput.COUNTRY.Value = "US"
	// 	mkOutput.COUNTRY.Source = "WM"
	// }
	// if len(mkOutput.STATE.Value) == 0 && mkOutput.COUNTRY.Value == "PR" { // handle libpostal treating PR as country
	// 	mkOutput.STATE.Value = "PR"
	// 	mkOutput.COUNTRY.Value = "US"
	// 	mkOutput.COUNTRY.Source = "WM"
	// }
}

func Contains(slice []string, item string) bool {
	for _, v := range slice {
		if strings.EqualFold(v, item) {
			return true
		}
	}
	return false
}

func LogDev(s string) {
	if dev {
		log.Printf(s)
	}
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
			return smartyStreetResponse
		}
	}
	return nil
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


type NameParsed struct {
	FNAME  string
	LNAME  string
	SUFFIX string
}

type Signature struct {
	OwnerID   string `json:"ownerId"`
	Source    string `json:"source"`
	EventID   string `json:"eventId"`
	EventType string `json:"eventType"`
	FiberType string `json:"fiberType"`
	RecordID  string `json:"recordId"`
	FiberID   string `json:"id"`
}

type Output struct {
	Signature   Signature         `json:"signature"`
	Passthrough map[string]string `json:"passthrough"`
	MatchKeys   PeopleOutput      `json:"matchkeys"`
}

type MatchKeyField struct {
	Value  string `json:"value"`
	Source string `json:"source"`
	Type   string `json:"type"`
}

type PeopleOutput struct {
	SALUTATION   MatchKeyField `json:"salutation"`
	NICKNAME     MatchKeyField `json:"nickname"`
	FNAME        MatchKeyField `json:"fname"`
	FINITIAL     MatchKeyField `json:"finitial"`
	MNAME        MatchKeyField `json:"mname"`
	LNAME        MatchKeyField `json:"lname"`
	FULLNAME     MatchKeyField `json:"-"` // do not output in json or store in BQ
	AD1          MatchKeyField `json:"ad1"`
	AD1NO        MatchKeyField `json:"ad1no"`
	AD2          MatchKeyField `json:"ad2"`
	AD3          MatchKeyField `json:"ad3"`
	CITY         MatchKeyField `json:"city"`
	STATE        MatchKeyField `json:"state"`
	ZIP          MatchKeyField `json:"zip"`
	ZIP5         MatchKeyField `json:"zip5"`
	COUNTRY      MatchKeyField `json:"country"`
	MAILROUTE    MatchKeyField `json:"mailroute"`
	ADTYPE       MatchKeyField `json:"adtype"`
	ZIPTYPE      MatchKeyField `json:"ziptype"`
	RECORDTYPE   MatchKeyField `json:"recordtype"`
	ADBOOK       MatchKeyField `json:"adbook"`
	ADPARSER     MatchKeyField `json:"adparser"`
	ADCORRECT    MatchKeyField `json:"adcorrect"`
	ADVALID      MatchKeyField `json:"advalid"`
	DORM         MatchKeyField `json:"-"` // do not output in json or store in BQ
	ROOM         MatchKeyField `json:"-"` // do not output in json or store in BQ
	FULLADDRESS  MatchKeyField `json:"-"` // do not output in json or store in BQ
	CITYSTATEZIP MatchKeyField `json:"-"` // do not output in json or store in BQ
	EMAIL        MatchKeyField `json:"email"`
	PHONE        MatchKeyField `json:"phone"`
	TRUSTEDID    MatchKeyField `json:"trustedId"`
	CLIENTID     MatchKeyField `json:"clientId"`
	GENDER       MatchKeyField `json:"gender"`
	AGE          MatchKeyField `json:"age"`
	DOB          MatchKeyField `json:"dob"`
	ORGANIZATION MatchKeyField `json:"organization"`
	TITLE        MatchKeyField `json:"title"`
	ROLE         MatchKeyField `json:"role"`
	STATUS       MatchKeyField `json:"status"`
	PermE        MatchKeyField `json:"PermE"`
	PermM        MatchKeyField `json:"PermM"`
	PermS        MatchKeyField `json:"PermS"`
}

type PubSubMessage struct {
	Data []byte `json:"data"`
}

func lookupState(in string) string { // THIS SHOULD BE RECODED AS MAP LOOKUP
	switch in {
	case "Alabama":
		return "AL"
	case "Alaska":
		return "AK"
	case "Arizona":
		return "AZ"
	case "Arkansas":
		return "AR"
	case "California":
		return "CA"
	case "Colorado":
		return "CO"
	case "Connecticut":
		return "CT"
	case "Delaware":
		return "DE"
	case "District Of Columbia":
		return "DC"
	case "Florida":
		return "FL"
	case "Georgia":
		return "GA"
	case "Hawaii":
		return "HI"
	case "Idaho":
		return "ID"
	case "Illinois":
		return "IL"
	case "Indiana":
		return "IN"
	case "Iowa":
		return "IA"
	case "Kansas":
		return "KS"
	case "Kentucky":
		return "KY"
	case "Louisiana":
		return "LA"
	case "Maine":
		return "ME"
	case "Maryland":
		return "MD"
	case "Massachusetts":
		return "MA"
	case "Michigan":
		return "MI"
	case "Minnesota":
		return "MN"
	case "Mississippi":
		return "MS"
	case "Missouri":
		return "MO"
	case "Montana":
		return "MN"
	case "Nebraska":
		return "NE"
	case "Nevada":
		return "NV"
	case "New Hampshire":
		return "NH"
	case "New Jersey":
		return "NJ"
	case "New Mexico":
		return "NM"
	case "New York":
		return "NY"
	case "North Carolina":
		return "NC"
	case "North Dakota":
		return "ND"
	case "Ohio":
		return "OH"
	case "Oklahoma":
		return "OK"
	case "Oregon":
		return "OR"
	case "Pennsylvania":
		return "PA"
	case "Rhode Island":
		return "RI"
	case "South Carolina":
		return "SC"
	case "South Dakota":
		return "SD"
	case "Tennessee":
		return "TN"
	case "Texas":
		return "TX"
	case "Utah":
		return "UT"
	case "Vermont":
		return "VT"
	case "Virginia":
		return "VA"
	case "Washington":
		return "WA"
	case "West Virginia":
		return "WV"
	case "Wisconsin":
		return "WI"
	case "Wyoming":
		return "WY"
	}
	return in
}

type PeopleSetDS struct {
	ID                     *datastore.Key `datastore:"__key__"`
	OwnerID                []string       `datastore:"ownerid"`
	Source                 []string       `datastore:"source"`
	EventID                []string       `datastore:"eventid"`
	EventType              []string       `datastore:"eventtype"`
	FiberType              []string       `datastore:"fibertype"`
	RecordID               []string       `datastore:"recordid"`
	RecordIDNormalized     []string       `datastore:"recordidnormalized"`
	CreatedAt              time.Time      `datastore:"createdat"`
	Fibers                 []string       `datastore:"fibers"`
	Search                 []string       `datastore:"search"`
	SALUTATION             []string       `datastore:"salutation"`
	SALUTATIONNormalized   []string       `datastore:"salutationnormalized"`
	NICKNAME               []string       `datastore:"nickname"`
	NICKNAMENormalized     []string       `datastore:"nicknamenormalized"`
	FNAME                  []string       `datastore:"fname"`
	FNAMENormalized        []string       `datastore:"fnamenormalized"`
	FINITIAL               []string       `datastore:"finitial"`
	FINITIALNormalized     []string       `datastore:"finitialnormalized"`
	LNAME                  []string       `datastore:"lname"`
	LNAMENormalized        []string       `datastore:"lnamenormalized"`
	MNAME                  []string       `datastore:"mname"`
	MNAMENormalized        []string       `datastore:"mnamenormalized"`
	AD1                    []string       `datastore:"ad1"`
	AD1Normalized          []string       `datastore:"ad1normalized"`
	AD1NO                  []string       `datastore:"ad1no"`
	AD1NONormalized        []string       `datastore:"ad1nonormalized"`
	AD2                    []string       `datastore:"ad2"`
	AD2Normalized          []string       `datastore:"ad2normalized"`
	AD3                    []string       `datastore:"ad3"`
	AD3Normalized          []string       `datastore:"ad3normalized"`
	AD4                    []string       `datastore:"ad4"`
	AD4Normalized          []string       `datastore:"ad4normalized"`
	CITY                   []string       `datastore:"city"`
	CITYNormalized         []string       `datastore:"citynormalized"`
	STATE                  []string       `datastore:"state"`
	STATENormalized        []string       `datastore:"statenormalized"`
	ZIP                    []string       `datastore:"zip"`
	ZIPNormalized          []string       `datastore:"zipnormalized"`
	ZIP5                   []string       `datastore:"zip5"`
	ZIP5Normalized         []string       `datastore:"zip5normalized"`
	COUNTRY                []string       `datastore:"country"`
	COUNTRYNormalized      []string       `datastore:"countrynormalized"`
	MAILROUTE              []string       `datastore:"mailroute"`
	MAILROUTENormalized    []string       `datastore:"mailroutenormalized"`
	ADTYPE                 []string       `datastore:"adtype"`
	ADTYPENormalized       []string       `datastore:"adtypenormalized"`
	ZIPTYPE                []string       `datastore:"ziptype"`
	ZIPTYPENormalized      []string       `datastore:"ziptypenormalized"`
	RECORDTYPE             []string       `datastore:"recordtype"`
	RECORDTYPENormalized   []string       `datastore:"recordtypenormalized"`
	ADBOOK                 []string       `datastore:"adbook"`
	ADBOOKNormalized       []string       `datastore:"adbooknormalized"`
	ADPARSER               []string       `datastore:"adparser"`
	ADPARSERNormalized     []string       `datastore:"adparsernormalized"`
	ADCORRECT              []string       `datastore:"adcorrect"`
	ADCORRECTNormalized    []string       `datastore:"adcorrectnormalized"`
	ADVALID                []string       `datastore:"advalid"`
	ADVALIDNormalized      []string       `datastore:"advalidnormalized"`
	EMAIL                  []string       `datastore:"email"`
	EMAILNormalized        []string       `datastore:"emailnormalized"`
	PHONE                  []string       `datastore:"phone"`
	PHONENormalized        []string       `datastore:"phonenormalized"`
	TRUSTEDID              []string       `datastore:"trustedid"`
	TRUSTEDIDNormalized    []string       `datastore:"trustedidnormalized"`
	CLIENTID               []string       `datastore:"clientid"`
	CLIENTIDNormalized     []string       `datastore:"clientidnormalized"`
	GENDER                 []string       `datastore:"gender"`
	GENDERNormalized       []string       `datastore:"gendernormalized"`
	AGE                    []string       `datastore:"age"`
	AGENormalized          []string       `datastore:"agenormalized"`
	DOB                    []string       `datastore:"dob"`
	DOBNormalized          []string       `datastore:"dobnormalized"`
	ORGANIZATION           []string       `datastore:"organization"`
	ORGANIZATIONNormalized []string       `datastore:"organizationnormalized"`
	TITLE                  []string       `datastore:"title"`
	TITLENormalized        []string       `datastore:"titlenormalized"`
	ROLE                   []string       `datastore:"role"`
	ROLENormalized         []string       `datastore:"rolenormalized"`
	STATUS                 []string       `datastore:"status"`
	STATUSNormalized       []string       `datastore:"statusnormalized"`
	PermE                  []string       `datastore:"perme"`
	PermENormalized        []string       `datastore:"permenormalized"`
	PermM                  []string       `datastore:"permm"`
	PermMNormalized        []string       `datastore:"permmnormalized"`
	PermS                  []string       `datastore:"perms"`
	PermSNormalized        []string       `datastore:"permsnormalized"`
}

func AppendRedisTempKey(keyparts []string, value string) {
	ms := msp.Get()
	defer ms.Close()

	_, err := ms.Do("APPEND", strings.Join(keyparts, ":"), value)
	if err != nil {
		log.Printf("Error APPEND value %v to %v, error %v", strings.Join(keyparts, ":"), value, err)
	}
	_, err = ms.Do("EXPIRE", strings.Join(keyparts, ":"), redisTemporaryExpiration)
	if err != nil {
		log.Printf("Error EXPIRE value %v to %v, error %v", strings.Join(keyparts, ":"), value, err)
	}
}