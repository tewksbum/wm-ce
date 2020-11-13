// Package streamerapi contains a series of cloud functions for streamer
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

	"cloud.google.com/go/pubsub"
	"googlemaps.github.io/maps"
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

var env = os.Getenv("ENVIRONMENT")
var dev = (env == "dev")

// global vars
var ctx context.Context
var ps *pubsub.Client
var topic *pubsub.Topic
var ap http.Client
var gm *maps.Client

func init() {
	ctx = context.Background()
	ps, _ = pubsub.NewClient(ctx, ProjectID)
	topic = ps.Topic(PubSubTopic)
	log.Printf("init completed, pubsub topic names: %v", topic)
}

// ProcessAddress Receives a http event request
func ProcessAddress(ctx context.Context, m PubSubMessage) error {
	var input Input
	if err := json.Unmarshal(m.Data, &input); err != nil {
		log.Fatalf("Error: Unable to unmarshal message %v with error %v", string(m.Data), err)
	}

	var output Output
	output.Signature = Signature{
		OwnerID:   input.Owner,
		Source:    input.Source,
		EventID:   input.EventID,
		EventType: input.EventType,
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

	StandardizeAddressSmartyStreet(&person)
	if person.ADVALID.Value != "TRUE" {
		StandardizeAddressGoogleMap(&person)
	}
	output.MatchKeys = person
	
	otherValues := make(map[string]string)
	otherValues["netsuite_key"] = input.NetsuiteKey
	output.Passthrough = otherValues

	outputJSON, _ := json.Marshal(output)

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