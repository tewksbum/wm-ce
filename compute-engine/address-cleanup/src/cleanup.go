package cleanup

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync"

	// cloud sql driver
	_ "github.com/go-sql-driver/mysql"
	"googlemaps.github.io/maps"
)

var db *sql.DB
var gm *maps.Client
var smartyStreetRequestURL string
var ctx context.Context
var reNewline = regexp.MustCompile(`\r?\n`)
var psCleaned *sql.Stmt
var psCleaned2 *sql.Stmt
var psCleakedFP *sql.Stmt

func init() {
	ctx = context.Background()
	var err error
	if len(os.Getenv("MYSQL_NETSUITE_PROD")) > 0 { // for local testing
		db, err = sql.Open("mysql", os.Getenv("MYSQL_NETSUITE_PROD"))
		if err != nil {
			log.Printf("error opening db %v", err)
		}
	}
	smartyStreetRequestURL = "https://us-street.api.smartystreets.com/street-address?auth-id=3b2108e7-6cc7-d56e-2cd5-868f99696558&auth-token=t87tJiGacE9eCf01zyZD&street=%v"
	gm, _ = maps.NewClient(maps.WithAPIKey("AIzaSyBvLGRj_RzXIPxo58X1XVtrOs1tc7FwABs"))
	psCleaned, err = db.Prepare(`
	INSERT INTO cleaned_packages (scan_key, ship_address1, ship_city, ship_state, ship_zip, corrected)
	values (?, ?, ?, ?, ?, ?)
	`)
	psCleaned2, err = db.Prepare(`
	INSERT INTO cleaned_fedex_historical_2 (tracking_number, ship_address1, ship_city, ship_state, ship_zip, corrected)
	values (?, ?, ?, ?, ?, ?)
	`)
	psCleakedFP, err = db.Prepare(`
	INSERT INTO cleaned_fedex_packages (tracking_number, ship_address1, ship_city, ship_state, ship_zip, corrected)
	values (?, ?, ?, ?, ?, ?)
	`)
}

type address struct {
	Address1 string
	City     string
	State    string
	Zip      string
}

func RunPackages() {
	addressCache := make(map[string]address)
	rows, err := db.Query(`
	select scan_key, ship_address1, ship_city, ship_state, ship_zip from packages
	`)
	if err != nil {
		log.Fatal(err)
	}

	N := 20
	wg := new(sync.WaitGroup)
	sem := make(chan struct{}, N)
	var mutex = &sync.Mutex{}

	defer rows.Close()
	var source [][]string

	for rows.Next() {

		var scanKey string
		var address1 string
		var city string
		var state string
		var zip string

		_ = rows.Scan(&scanKey, &address1, &city, &state, &zip)
		address1 = strings.TrimSpace(address1)
		city = strings.TrimSpace(city)
		state = strings.TrimSpace(state)
		zip = strings.TrimSpace(zip)
		source = append(source, []string{
			scanKey, address1, city, state, zip,
		})
	}
	for _, entry := range source {
		var cleaned address
		scanKey := entry[0]
		address1 := entry[1]
		city := entry[2]
		state := entry[3]
		zip := entry[4]
		mapKey := strings.ToLower(address1) + strings.ToLower(city) + strings.ToLower(state) + strings.ToLower(zip)
		wg.Add(1)
		go func(scanKey string, address1 string, city string, state string, zip string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() {
				// Reading from the channel decrements the semaphore
				// (frees up buffer slot).
				<-sem
			}()
			if address, ok := addressCache[mapKey]; ok {
				cleaned = address
			} else {
				cleaned = StandardizeAddressSmartyStreet(address1, city, state, zip)
				if len(cleaned.Address1) == 0 {
					cleaned = StandardizeAddressGoogleMap(address1, city, state, zip)
				}
				if len(cleaned.Address1) > 0 {
					mutex.Lock()
					addressCache[mapKey] = cleaned
					mutex.Unlock()
				}
			}
			if len(cleaned.Address1) == 0 {
				_, err := psCleaned.Exec(scanKey, address1, city, state, zip, 0)
				if err != nil {
					log.Fatalf("Error inserting into table %v", err)
				}
			} else {
				_, err := psCleaned.Exec(scanKey, cleaned.Address1, cleaned.City, cleaned.State, cleaned.Zip, 1)
				if err != nil {
					log.Fatalf("Error inserting into table %v", err)
				}
			}
		}(scanKey, address1, city, state, zip)
		wg.Wait()
	}
}

func RunFedexHistoical2() {
	addressCache := make(map[string]address)
	rows, err := db.Query(`
	select Shipment_Tracking_Number, Recipient_Address, Recipient_City, Recipient_StateProvince, Recipient_Postal_Code from fedex_historical_2
	`)
	if err != nil {
		log.Fatal(err)
	}

	N := 20
	wg := new(sync.WaitGroup)
	sem := make(chan struct{}, N)
	var mutex = &sync.Mutex{}

	defer rows.Close()
	var source [][]string

	for rows.Next() {

		var scanKey string
		var address1 string
		var city string
		var state string
		var zip string

		_ = rows.Scan(&scanKey, &address1, &city, &state, &zip)
		address1 = strings.TrimSpace(address1)
		city = strings.TrimSpace(city)
		state = strings.TrimSpace(state)
		zip = strings.TrimSpace(zip)
		source = append(source, []string{
			scanKey, address1, city, state, zip,
		})
	}
	for _, entry := range source {
		var cleaned address
		scanKey := entry[0]
		address1 := entry[1]
		city := entry[2]
		state := entry[3]
		zip := entry[4]
		mapKey := strings.ToLower(address1) + strings.ToLower(city) + strings.ToLower(state) + strings.ToLower(zip)
		wg.Add(1)
		go func(scanKey string, address1 string, city string, state string, zip string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() {
				// Reading from the channel decrements the semaphore
				// (frees up buffer slot).
				<-sem
			}()
			if address, ok := addressCache[mapKey]; ok {
				cleaned = address
			} else {
				cleaned = StandardizeAddressSmartyStreet(address1, city, state, zip)
				if len(cleaned.Address1) == 0 {
					cleaned = StandardizeAddressGoogleMap(address1, city, state, zip)
				}
				if len(cleaned.Address1) > 0 {
					mutex.Lock()
					addressCache[mapKey] = cleaned
					mutex.Unlock()
				}
			}
			if len(cleaned.Address1) == 0 {
				_, err := psCleaned2.Exec(scanKey, address1, city, state, zip, 0)
				if err != nil {
					log.Fatalf("Error inserting into table %v", err)
				}
			} else {
				_, err := psCleaned2.Exec(scanKey, cleaned.Address1, cleaned.City, cleaned.State, cleaned.Zip, 1)
				if err != nil {
					log.Fatalf("Error inserting into table %v", err)
				}
			}
		}(scanKey, address1, city, state, zip)
		wg.Wait()
	}
}

func RunFedexPackages() {
	addressCache := make(map[string]address)
	rows, err := db.Query(`
	select tracking_number, receiver_addr1, receiver_city, receiver_state, receiver_zip from fedex_packages
	`)
	if err != nil {
		log.Fatal(err)
	}

	N := 20
	wg := new(sync.WaitGroup)
	sem := make(chan struct{}, N)
	var mutex = &sync.Mutex{}

	defer rows.Close()
	var source [][]string

	for rows.Next() {

		var scanKey string
		var address1 string
		var city string
		var state string
		var zip string

		_ = rows.Scan(&scanKey, &address1, &city, &state, &zip)
		address1 = strings.TrimSpace(address1)
		city = strings.TrimSpace(city)
		state = strings.TrimSpace(state)
		zip = strings.TrimSpace(zip)
		source = append(source, []string{
			scanKey, address1, city, state, zip,
		})
	}
	for _, entry := range source {
		var cleaned address
		scanKey := entry[0]
		address1 := entry[1]
		city := entry[2]
		state := entry[3]
		zip := entry[4]
		mapKey := strings.ToLower(address1) + strings.ToLower(city) + strings.ToLower(state) + strings.ToLower(zip)
		wg.Add(1)
		go func(scanKey string, address1 string, city string, state string, zip string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() {
				// Reading from the channel decrements the semaphore
				// (frees up buffer slot).
				<-sem
			}()
			if address, ok := addressCache[mapKey]; ok {
				cleaned = address
			} else {
				cleaned = StandardizeAddressSmartyStreet(address1, city, state, zip)
				if len(cleaned.Address1) == 0 {
					cleaned = StandardizeAddressGoogleMap(address1, city, state, zip)
				}
				if len(cleaned.Address1) > 0 {
					mutex.Lock()
					addressCache[mapKey] = cleaned
					mutex.Unlock()
				}
			}
			if len(cleaned.Address1) == 0 {
				_, err := psCleakedFP.Exec(scanKey, address1, city, state, zip, 0)
				if err != nil {
					log.Fatalf("Error inserting into table %v", err)
				}
			} else {
				_, err := psCleakedFP.Exec(scanKey, cleaned.Address1, cleaned.City, cleaned.State, cleaned.Zip, 1)
				if err != nil {
					log.Fatalf("Error inserting into table %v", err)
				}
			}
		}(scanKey, address1, city, state, zip)
		wg.Wait()
	}
}

func StandardizeAddressGoogleMap(address1 string, city string, state string, zip string) address {
	addressInput := address1 + ", " + city + ", " + state + ", " + zip

	if len(strings.TrimSpace(addressInput)) > 10 {
		gmResult, err := gm.Geocode(ctx, &maps.GeocodingRequest{
			Address: addressInput,
		})
		if err != nil {
			log.Printf("Google Maps error %v", err)
		}

		if len(gmResult) > 0 && len(gmResult[0].FormattedAddress) > 0 {
			// pick the first result
			// log.Printf("Google Maps returned %v", gmResult[0].FormattedAddress)
			streetNumber := ""
			streetName := ""
			city := ""
			state := ""
			zip := ""
			for _, component := range gmResult[0].AddressComponents {
				if Contains(component.Types, "street_number") {
					streetNumber = component.ShortName
				} else if Contains(component.Types, "route") {
					streetName = component.ShortName
				} else if Contains(component.Types, "locality") {
					city = component.ShortName
				} else if Contains(component.Types, "administrative_area_level_1") {
					state = component.ShortName
				} else if Contains(component.Types, "postal_code") {
					zip = component.ShortName
				}
			}
			ad := address{
				Address1: streetNumber + " " + streetName,
				City:     city,
				State:    state,
				Zip:      zip,
			}
			return ad
		}

	}
	return address{}

}
func Contains(slice []string, item string) bool {
	for _, v := range slice {
		if strings.EqualFold(v, item) {
			return true
		}
	}
	return false
}

func CorrectAddress(in string) SmartyStreetResponse {
	var smartyStreetResponse SmartyStreetResponse
	requestURL := fmt.Sprintf(smartyStreetRequestURL, url.QueryEscape(in))
	response, err := http.Get(requestURL)
	if err != nil {
		log.Printf("smartystreet request failed: %v", err)
	} else {
		if response.StatusCode != 200 {
			log.Printf("smartystreet request failed, status code:%v", response.StatusCode)
		}
		data, err := ioutil.ReadAll(response.Body)
		if err != nil {
			log.Printf("Couldn't read the smartystreet response: %v", err)
		}
		// log.Printf("smartystreet response %v", string(data))
		json.Unmarshal(data, &smartyStreetResponse)

		if len(smartyStreetResponse) > 0 {
			return smartyStreetResponse
		}
	}
	return nil
}

func StandardizeAddressSmartyStreet(address1 string, city string, state string, zip string) address {
	addressInput := address1 + ", " + city + ", " + state + ", " + zip

	if len(strings.TrimSpace(addressInput)) > 10 {
		a := CorrectAddress(reNewline.ReplaceAllString(addressInput, ""))

		if len(a) > 0 && len(a[0].DeliveryLine1) > 1 { // take the first
			ad := address{
				City:     a[0].Components.CityName,
				State:    a[0].Components.StateAbbreviation,
				Zip:      a[0].Components.Zipcode,
				Address1: strings.TrimSpace(a[0].DeliveryLine1),
			}
			return ad
		}
	}
	return address{}
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
