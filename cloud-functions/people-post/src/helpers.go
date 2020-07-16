package peoplepost

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unicode"

	"googlemaps.github.io/maps"

	"cloud.google.com/go/pubsub"
	"github.com/fatih/structs"
	"github.com/gomodule/redigo/redis"
)

func publishReport(report *FileReport, cfName string) {
	reportJSON, _ := json.Marshal(report)
	reportPub := topicR.Publish(ctx, &pubsub.Message{
		Data: reportJSON,
		Attributes: map[string]string{
			"source": cfName,
		},
	})
	_, err := reportPub.Get(ctx)
	if err != nil {
		log.Printf("ERROR Could not pub to reporting pubsub: %v", err)
	}
}

func GetPopulatedMatchKeys(a *PeopleOutput) []string {
	names := structs.Names(&PeopleOutput{})
	result := []string{}
	for _, n := range names {
		mk := GetMkField(a, n)
		if len(mk.Value) > 0 {
			result = append(result, n)
		}
	}
	return result
}

func columnMatchOverride(column InputColumn, titleValue string, parsedName NameParsed) string {

	column.MatchKey1 = "" //maybe not needed?

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
	} else if column.PeopleERR.ContainsStudentRole == 1 && column.IsAttribute { //preffer the attribute.
		// TODO: a contains here seems VERY dangerous...
		column.MatchKey1 = "ROLE"
		LogDev(fmt.Sprintf("MatchKey %v on condition %v", column.MatchKey1, "column.PeopleERR.ContainsStudentRole == 1 && column.IsAttribute"))
	} else if (column.PeopleERR.Title == 1 || column.PeopleERR.ContainsTitle == 1) && !reNameTitle.MatchString(column.Value) {
		// !reNameTitle makes sure the value is not like a Mr. Mrs.
		// we don't want to overwrite a file supplied TITLE w/ an attribute...
		if !column.IsAttribute {
			column.MatchKey1 = "TITLE"
		} else if column.IsAttribute && titleValue == "" {
			column.MatchKey1 = "TITLE"
		}
		LogDev(fmt.Sprintf("MatchKey %v on condition %v and %v", column.MatchKey1, column.MatchKey2, " column.PeopleERR.Title == 1 || column.PeopleERR.ContainsTitle == 1"))
		// column.MatchKey = ""
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

	// this might be a full name, try to parse it and see if we have first and last names
	// || (column.PeopleVER.IS_FIRSTNAME && column.PeopleVER.IS_LASTNAME && column.PeopleERR.ContainsName == 1)
	if column.PeopleERR.ContainsRole == 1 || column.PeopleERR.FullName == 1 || (column.PeopleVER.IS_FIRSTNAME && column.PeopleVER.IS_LASTNAME && ((column.PeopleERR.ContainsFirstName == 1 && column.PeopleERR.ContainsLastName == 1) || (column.PeopleERR.ContainsFirstName == 0 && column.PeopleERR.ContainsLastName == 0))) {
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
		} else if column.PeopleVER.IS_COUNTRY && (column.PeopleERR.Country == 1 || column.PeopleERR.Address2 == 1 || column.PeopleERR.Address3 == 1 || column.PeopleERR.Address4 == 1 || column.PeopleERR.ContainsCountry == 1) {
			column.MatchKey1 = "COUNTRY"
			LogDev(fmt.Sprintf("Country: %v", column.Value))
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
		} else if column.PeopleERR.ContainsFirstName == 1 && column.PeopleVER.IS_FIRSTNAME && column.PeopleERR.Junk == 0 {
			column.MatchKey1 = "FNAME"
			LogDev(fmt.Sprintf("MatchKey %v on condition %v", column.MatchKey1, "column.PeopleERR.ContainsFirstName == 1 && column.PeopleVER.IS_FIRSTNAME"))
		} else if column.PeopleERR.FirstName == 1 {
			column.MatchKey1 = "FNAME"
			LogDev(fmt.Sprintf("MatchKey %v on condition %v", column.MatchKey1, "column.PeopleERR.FirstName == 1"))
		} else if column.PeopleERR.MiddleName == 1 {
			column.MatchKey1 = "MNAME"
			LogDev(fmt.Sprintf("MatchKey %v on condition %v", column.MatchKey1, "column.PeopleERR.MiddleName == 1"))
		} else if column.PeopleERR.ContainsLastName == 1 && column.PeopleVER.IS_LASTNAME && column.PeopleERR.Junk == 0 {
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
		} else if column.PeopleERR.State == 1 || (column.PeopleERR.ContainsRole == 1 && column.PeopleERR.ContainsState == 1) && column.PeopleERR.Junk == 0 {
			column.MatchKey1 = "STATE"
			LogDev(fmt.Sprintf("MatchKey %v on condition %v", column.MatchKey1, "column.PeopleERR.State == 1"))
		} else if column.PeopleERR.ZipCode == 1 {
			column.MatchKey1 = "ZIP"
			LogDev(fmt.Sprintf("MatchKey %v on condition %v", column.MatchKey1, "column.PeopleERR.ZipCode == 1"))
			// start of the VER checks...
		} else if column.PeopleVER.IS_STREET1 && column.PeopleERR.Junk == 0 && column.PeopleERR.ContainsAddress == 1 && column.PeopleERR.ContainsCountry == 0 {
			column.MatchKey1 = "AD1"
			LogDev(fmt.Sprintf("acd - MatchKey %v on condition %v", column.MatchKey1, "column.PeopleVER.IS_STREET1 && column.PeopleERR.Junk == 0 && column.PeopleERR.ContainsCountry == 0"))
		} else if column.PeopleVER.IS_STREET2 && column.PeopleERR.Junk == 0 && column.PeopleERR.ContainsAddress == 1 && column.PeopleERR.ContainsCountry == 0 && !column.PeopleVER.IS_COUNTRY {
			column.MatchKey1 = "AD2"
			LogDev(fmt.Sprintf("acd - MatchKey %v on condition %v", column.MatchKey1, "column.PeopleVER.IS_STREET2 && column.PeopleERR.Junk == 0 && column.PeopleERR.ContainsCountry == 0"))
		} else if column.PeopleVER.IS_STREET3 && column.PeopleERR.Junk == 0 && column.PeopleERR.ContainsAddress == 1 && column.PeopleERR.ContainsCountry == 0 {
			column.MatchKey1 = "AD3"
			LogDev(fmt.Sprintf("acd - MatchKey %v on condition %v", column.MatchKey1, "column.PeopleVER.IS_STREET3 && column.PeopleERR.Junk == 0 && column.PeopleERR.ContainsCountry == 0"))
		} else if column.PeopleVER.IS_STATE && column.PeopleERR.ContainsState == 1 && column.PeopleERR.Junk == 0 && column.PeopleERR.MiddleName == 0 {
			column.MatchKey1 = "STATE"
			LogDev(fmt.Sprintf("acd - MatchKey %v on condition %v", column.MatchKey1, "column.PeopleVER.IS_STATE && column.PeopleERR.Junk == 0 && column.PeopleERR.MiddleName == 0"))
		} else if column.PeopleVER.IS_ZIPCODE && column.PeopleERR.ContainsZipCode == 1 && column.PeopleERR.Junk == 0 {
			column.MatchKey1 = "ZIP"
			LogDev(fmt.Sprintf("acd - MatchKey %v on condition %v", column.MatchKey1, "column.PeopleVER.IS_ZIPCODE && column.PeopleERR.ContainsZipCode == 1 && column.PeopleERR.Junk == 0"))
		} else if column.PeopleVER.IS_CITY && column.PeopleERR.ContainsCity == 1 && column.PeopleERR.Junk == 0 && column.PeopleERR.ContainsFirstName == 0 && column.PeopleERR.ContainsLastName == 0 && column.PeopleERR.MiddleName == 0 && column.PeopleERR.Gender == 0 && column.PeopleERR.ContainsRole == 0 && column.PeopleERR.County == 0 && column.PeopleERR.ContainsCountry == 0 {
			column.MatchKey1 = "CITY"
			LogDev(fmt.Sprintf("acd - MatchKey %v on condition %v", column.MatchKey1, "column.PeopleVER.IS_CITY && column.PeopleERR.Junk == 0 && column.PeopleERR.ContainsFirstName == 0 && column.PeopleERR.ContainsLastName == 0 && column.PeopleERR.MiddleName == 0 && column.PeopleERR.Gender == 0 && column.PeopleERR.ContainsCountry == 0"))
		} else if column.PeopleVER.IS_COUNTRY && column.PeopleERR.ContainsCountry == 1 {
			column.MatchKey1 = "COUNTRY"
			LogDev(fmt.Sprintf("acd - MatchKey %v on condition %v", column.MatchKey1, "column.PeopleVER.IS_COUNTRY"))
		} else if column.PeopleERR.ContainsFirstName == 1 && column.PeopleERR.Junk == 0 {
			column.MatchKey1 = "FNAME"
			LogDev(fmt.Sprintf("acd - MatchKey %v on condition %v", column.MatchKey1, "column.PeopleERR.ContainsFirstName == 1"))
		} else if column.PeopleERR.ContainsLastName == 1 && column.PeopleERR.Junk == 0 {
			column.MatchKey1 = "LNAME"
			LogDev(fmt.Sprintf("acd - MatchKey %v on condition %v", column.MatchKey1, "column.PeopleERR.ContainsLastName == 1"))
		} else if column.PeopleERR.ContainsCity == 1 && column.PeopleERR.Junk == 0 && column.PeopleERR.Gender == 0 {
			column.MatchKey1 = "CITY"
			LogDev(fmt.Sprintf("acd - MatchKey %v on condition %v", column.MatchKey1, "column.PeopleERR.ContainsCity == 1 && column.PeopleERR.Junk == 0 && column.PeopleERR.Gender == 0"))
		} else if column.PeopleERR.ContainsAddress == 1 && column.PeopleERR.Junk == 0 {
			// we should likely dump this one...
			column.MatchKey1 = "AD1"
			LogDev(fmt.Sprintf("acd - BAD MatchKey %v on condition %v", column.MatchKey1, "column.PeopleERR.ContainsAddress == 1 && column.PeopleERR.Junk == 0"))
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

	// clear MatchKey if Junk
	if column.PeopleERR.Junk == 1 {
		LogDev(fmt.Sprintf("JUNK is dropping your match keys: %v %v %v %v", column.MatchKey, column.MatchKey1, column.MatchKey2, column.MatchKey3))
		column.MatchKey = ""
		column.MatchKey1 = ""
		column.MatchKey2 = ""
		column.MatchKey3 = ""
	}

	return column.MatchKey1
}

func CopyFieldsToMPR(a *PeopleOutput, b *PeopleOutput) {
	r := reflect.ValueOf(a)
	w := reflect.ValueOf(b)
	v := reflect.Indirect(r)
	z := reflect.Indirect(w)
	e := v.Type()
	for i := 0; i < v.NumField(); i++ {
		name := e.Field(i).Name
		if name != "EMAIL" && name != "PHONE" && name != "FNAME" { // do not copy email and phone and fname
			s := v.FieldByName(name).Interface().(MatchKeyField)
			t := z.FieldByName(name).Interface().(MatchKeyField)
			if len(t.Value) == 0 {
				z.FieldByName(e.Field(i).Name).Set(reflect.ValueOf(s))
			}
		}
	}
}

func Contains(slice []string, item string) bool {
	for _, v := range slice {
		if strings.EqualFold(v, item) {
			return true
		}
	}
	return false
}

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
			mkOutput.COUNTRY.Value = "US"                          // if libpostal can parse it, it is an US address
			SetMkField(mkOutput, "ADPARSER", "smartystreet", "SS") // if libpostal can parse it, it is an US address
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
			// correctedAddress.AddressType = smartyStreetResponse[0].Metadata.Rdi

			return smartyStreetResponse
		}
	}
	return nil
}

func lookupState(in string) string {
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
	f.Set(reflect.ValueOf(MatchKeyField{Value: strings.TrimSpace(value), Source: source}))
	if dev {
		log.Printf("SetMkField: %v %v %v", field, value, source)
		log.Printf("MkField %v", GetMkField(v, field))
	}
}

func SetMkFieldWithType(v *PeopleOutput, field string, value string, source string, t string) {
	r := reflect.ValueOf(v)
	f := reflect.Indirect(r).FieldByName(field)

	f.Set(reflect.ValueOf(MatchKeyField{Value: strings.TrimSpace(value), Source: source, Type: t}))
	if dev {
		log.Printf("SetMkField: %v %v %v %v", field, value, source, t)
		log.Printf("MkField %v", GetMkField(v, field))
	}
}

// intended to be part of address correction
// func checkCityStateZip(city string, state string, zip string) bool {
// 	checkZip := zip
// 	if len(checkZip) > 5 {
// 		checkZip = checkZip[0:5]
// 	}
// 	checkCity := strings.TrimSpace(strings.ToLower(city))
// 	checkState := strings.TrimSpace(strings.ToLower(state))
// 	var result bool
// 	result = false

// 	// TODO: store this in binary search tree or something
// 	for _, item := range listCityStateZip {
// 		if IndexOf(checkCity, item.Cities) > -1 && checkState == item.State && checkZip == item.Zip {
// 			return true
// 		}
// 	}
// 	return result
// }

// func populateCityStateFromZip(zip string) (string, string) {
// 	checkZip := zip
// 	if len(checkZip) >= 5 {
// 		checkZip = checkZip[0:5]
// 	}
// 	if cs, ok := zipMap[checkZip]; ok {
// 		return cs.City, cs.State
// 	}
// 	return "", ""
// }

// func readZipMap(ctx context.Context, client *storage.Client, bucket, object string) (map[string]CityState, error) {
// 	result := make(map[string]CityState)
// 	cszList, err := readCityStateZip(ctx, client, bucket, object)
// 	if err != nil {
// 		log.Printf("error loading city state zip list %v", err)

// 	} else {
// 		for _, csz := range cszList {
// 			result[csz.Zip] = CityState{
// 				City:  (csz.Cities)[0],
// 				State: csz.State,
// 			}
// 		}
// 	}
// 	return result, nil
// }

// intended to be part of address correction
// func readCityStateZip(ctx context.Context, client *storage.Client, bucket, object string) ([]CityStateZip, error) {
// 	var result []CityStateZip
// 	rc, err := client.Bucket(bucket).Object(object).NewReader(ctx)
// 	if err != nil {
// 		return nil, err
// 	}
// 	defer rc.Close()

// 	data, err := ioutil.ReadAll(rc)
// 	if err != nil {
// 		return nil, err
// 	}
// 	json.Unmarshal(data, &result)
// 	return result, nil
// }

func IndexOf(element string, data []string) int {
	for k, v := range data {
		if element == v {
			return k
		}
	}
	return -1 //not found.
}

func StandardizeAddressLP(mkOutput *PeopleOutput) {
	STATEValue := mkOutput.STATE.Value
	CITYValue := mkOutput.CITY.Value
	addressInput := mkOutput.AD1.Value + ", " + mkOutput.AD2.Value + ", " + mkOutput.CITY.Value + ", " + mkOutput.STATE.Value + " " + mkOutput.ZIP.Value + ", " + mkOutput.COUNTRY.Value
	LogDev(fmt.Sprintf("addressInput passed TO parser %v", addressInput))
	if len(strings.TrimSpace(addressInput)) > 0 {
		a := ParseAddress(reNewline.ReplaceAllString(addressInput, ""))
		LogDev(fmt.Sprintf("address parser returned %v from input %v", a, addressInput))
		if len(a.CITY) > 0 || len(a.CITY_DISTRICT) > 0 {
			mkOutput.CITY.Value = strings.ToUpper(a.CITY)
			if len(a.CITY) == 0 && len(a.CITY_DISTRICT) > 0 {
				mkOutput.CITY.Value = strings.ToUpper(a.CITY_DISTRICT)
			}
			mkOutput.STATE.Value = strings.ToUpper(a.STATE)
			mkOutput.ZIP.Value = strings.ToUpper(a.POSTCODE)
			if len(a.COUNTRY) > 0 {
				mkOutput.COUNTRY.Value = strings.ToUpper(a.COUNTRY)
			}
			mkOutput.ADPARSER.Value = "libpostal"
			if len(a.PO_BOX) > 0 {
				if len(a.HOUSE_NUMBER) > 0 {
					mkOutput.AD1.Value = strings.TrimSpace(strings.ToUpper(a.HOUSE_NUMBER + " " + a.ROAD + " " + a.SUBURB))
					mkOutput.AD1NO.Value = strings.ToUpper(a.HOUSE_NUMBER)
					LogDev(fmt.Sprintf("StandardizeAddress po comparison: %v %v", strings.ToUpper(mkOutput.AD1.Value), strings.ToUpper(a.PO_BOX)))
					if strings.ToUpper(mkOutput.AD1.Value) != strings.ToUpper(a.PO_BOX) {
						mkOutput.AD2.Value = strings.ToUpper(a.PO_BOX)
					}
				} else {
					mkOutput.AD1.Value = strings.ToUpper(a.PO_BOX)
					mkOutput.AD1NO.Value = strings.TrimPrefix(a.PO_BOX, "PO BOX ")
				}
			} else {
				mkOutput.AD1.Value = strings.TrimSpace(strings.ToUpper(a.HOUSE_NUMBER + " " + a.ROAD + " " + a.SUBURB))
				mkOutput.AD1NO.Value = strings.ToUpper(a.HOUSE_NUMBER)
				mkOutput.AD2.Value = strings.ToUpper(a.LEVEL) + " " + strings.ToUpper(a.UNIT)
			}
			if reState.MatchString(a.STATE) {
				LogDev(fmt.Sprintf("overriding country by state value: %v", a.STATE))
				mkOutput.COUNTRY.Value = "US"
				mkOutput.COUNTRY.Source = "WM"
			}
			if len(a.STATE) == 0 && mkOutput.COUNTRY.Value == "PR" { // handle libpostal treating PR as country
				mkOutput.STATE.Value = "PR"
				mkOutput.COUNTRY.Value = "US"
				mkOutput.COUNTRY.Source = "WM"
			}

			if (len(mkOutput.STATE.Value) == 0 && len(STATEValue) > 0) || (len(mkOutput.CITY.Value) == 0 && len(CITYValue) > 0) {
				mkOutput.STATE.Value = strings.ToUpper(STATEValue)
				mkOutput.CITY.Value = strings.ToUpper(CITYValue)
			}
		}
	}
}

// DEPRECATED, keeping for reference
// func AddressParse(mko *PeopleOutput, input *Input, concatCityState bool, concatCityStateCol int, concatAdd bool, concatAddCol int) {
// 	var addressInput string

// 	if !concatCityState && !concatAdd {
// 		addressInput = mko.AD1.Value + " " + mko.AD2.Value + " " + mko.CITY.Value + " " + mko.STATE.Value + " " + mko.ZIP.Value
// 		if dev {
// 			log.Printf("!concatAdd + !concatCityState %v ", addressInput)
// 		}
// 	} else if !concatAdd && concatCityState {
// 		addressInput = mko.AD1.Value + " " + mko.AD2.Value + " " + input.Columns[concatCityStateCol].Value
// 		if dev {
// 			log.Printf("!concatAdd + concatCityState %v ", addressInput)
// 		}
// 	} else if concatAdd && !concatCityState {
// 		addressInput = input.Columns[concatAddCol].Value
// 		if dev {
// 			log.Printf("concatAdd + !concatCityState %v ", addressInput)
// 		}
// 	} else if concatAdd && concatCityState {
// 		// this is potentially duplicate data?
// 		addressInput = input.Columns[concatAddCol].Value + input.Columns[concatCityStateCol].Value
// 		if dev {
// 			log.Printf("concatAdd + concatCityState %v ", addressInput)
// 		}
// 	}
// 	if len(strings.TrimSpace(addressInput)) > 0 {
// 		a := ParseAddress(addressInput)
// 		log.Printf("address parser returned %v", a)
// 		if len(a.CITY) > 0 || len(a.CITY_DISTRICT) > 0 {
// 			if len(a.CITY) > 0 {
// 				mko.CITY.Value = strings.ToUpper(a.CITY)
// 			} else {
// 				mko.CITY.Value = strings.ToUpper(a.CITY_DISTRICT)
// 			}
// 			mko.STATE.Value = strings.ToUpper(a.STATE)
// 			mko.ZIP.Value = strings.ToUpper(a.POSTCODE)
// 			if len(a.COUNTRY) > 0 {
// 				mko.COUNTRY.Value = strings.ToUpper(a.COUNTRY)
// 			}
// 			mko.ADPARSER.Value = "libpostal"
// 			if len(a.PO_BOX) > 0 {
// 				if len(a.HOUSE_NUMBER) > 0 {
// 					mko.AD1.Value = strings.TrimSpace(strings.ToUpper(a.HOUSE_NUMBER + " " + a.ROAD + " " + a.SUBURB))
// 					mko.AD1NO.Value = strings.ToUpper(a.HOUSE_NUMBER)
// 					mko.AD2.Value = strings.ToUpper(a.PO_BOX)
// 				} else {
// 					mko.AD1.Value = strings.ToUpper(a.PO_BOX)
// 					mko.AD1NO.Value = strings.TrimPrefix(a.PO_BOX, "PO BOX ")
// 				}
// 			} else {
// 				mko.AD1.Value = strings.ToUpper(a.HOUSE_NUMBER + " " + a.ROAD)
// 				mko.AD1NO.Value = strings.ToUpper(a.HOUSE_NUMBER)
// 				mko.AD2.Value = strings.ToUpper(a.LEVEL) + " " + strings.ToUpper(a.UNIT)
// 			}
// 			if reState.MatchString(a.STATE) {
// 				SetMkField(mko, "COUNTRY", "US", "WM")
// 			}
// 		}
// 	}

// }

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
	if column.PeopleERR.AddressTypeBusiness == 1 {
		return "Business"
	} else if column.PeopleERR.AddressTypeCampus == 1 {
		return "Campus"
	} else if column.PeopleERR.AddressTypeResidence == 1 {
		return "Residence"
	}
	return ""
}

func AssignAddressBook(column *InputColumn) string {
	if column.PeopleERR.AddressBookBill == 1 {
		return "Bill"
	} else if column.PeopleERR.AddressBookShip == 1 {
		return "Ship"
	}
	return "Bill"
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

func PubRecord(ctx context.Context, input *Input, mkOutput PeopleOutput, suffix string, recordType string) {
	var output Output
	output.Signature = input.Signature
	output.Signature.FiberType = recordType
	if len(suffix) > 0 {
		output.Signature.RecordID += suffix
	}
	output.Passthrough = input.Passthrough

	output.MatchKeys = mkOutput

	outputJSON, _ := json.Marshal(output)
	if recordType == "mar" {
		psresult := martopic.Publish(ctx, &pubsub.Message{
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
	} else {
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

}

func SetLibPostalField(v *LibPostalParsed, field string, value string) string {
	r := reflect.ValueOf(v)
	f := reflect.Indirect(r).FieldByName(field)
	f.SetString(value)
	return value
}

func CalcClassYear(title string, sy string, status string, flag bool) (string, string) {
	LogDev(fmt.Sprintf("calculating classyear and status: %v %v", title, sy))
	if reGraduationYear.MatchString(title) {
		return title, CalcClassStatus(title, status)
	} else if reClassYearFY1.MatchString(title) { // FY1617
		twodigityear, err := strconv.Atoi(title[2:4])
		if err == nil {
			title = strconv.Itoa(2000 + twodigityear + 4)
			return title, CalcClassStatus(title, status)
		}
	} else if reGraduationYear2.MatchString(title) { // given us a 2 year like "20"
		twodigityear, err := strconv.Atoi(title)
		if err == nil {
			title = strconv.Itoa(2000 + twodigityear)
			return title, CalcClassStatus(title, status)
		}
	}

	if sy != "" {
		twodigityear, err := strconv.Atoi(sy[0:2])
		if err == nil {
			// would be better to move these VER conditionals to regex....
			if reFreshman.MatchString(title) {
				return strconv.Itoa(2000 + twodigityear + 4), "undergraduate"
			} else if reSophomore.MatchString(title) {
				return strconv.Itoa(2000 + twodigityear + 3), "undergraduate"
			} else if reJunior.MatchString(title) {
				return strconv.Itoa(2000 + twodigityear + 2), "undergraduate"
			} else if reSenior.MatchString(title) {
				return strconv.Itoa(2000 + twodigityear + 1), "undergraduate"
			} else if reGraduate.MatchString(title) {
				return strconv.Itoa(2000 + twodigityear - 1), "graduate"
			}
		}
	}

	//It is only one try
	if titleYearAttr != "" && flag {
		return CalcClassYear(titleYearAttr, sy, status, false)
	}

	// if we can't fine a class match... then don't return one...
	return "", ""
}

func CalcClassStatus(cy string, status string) string {
	LogDev(fmt.Sprintf("Calculating status classyear: %v status: %v", cy, status))
	//If we have status attribute we use it.
	if len(status) > 0 {
		return status
	}
	//If not we calculate it.
	digityear, err := strconv.Atoi(cy)
	if err == nil {
		if digityear < time.Now().Year() {
			return "graduated"
		} else if digityear == time.Now().Year() && time.Now().Month() > 4 {
			return "graduated"
		}
		return "ungraduated"
	}
	return ""
}

func ParseName(v string) NameParsed {
	result := strings.Replace(v, "&", ",", -1)
	result = reFullName.FindStringSubmatch(result)
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

func SetRedisValueWithExpiration(keyparts []string, value int) {
	ms := msp.Get()
	defer ms.Close()

	_, err := ms.Do("SETEX", strings.Join(keyparts, ":"), redisTransientExpiration, value)
	if err != nil {
		log.Printf("Error setting redis value %v to %v, error %v", strings.Join(keyparts, ":"), value, err)
	}
}

func SetRedisTempKey(keyparts []string) {
	ms := msp.Get()
	defer ms.Close()

	_, err := ms.Do("SETEX", strings.Join(keyparts, ":"), redisTemporaryExpiration, 1)
	if err != nil {
		log.Printf("Error SETEX value %v to %v, error %v", strings.Join(keyparts, ":"), 1, err)
	}
}

func SetRedisTempKeyWithValue(keyparts []string, value string) {
	ms := msp.Get()
	defer ms.Close()

	_, err := ms.Do("SETEX", strings.Join(keyparts, ":"), redisTemporaryExpiration, value)
	if err != nil {
		log.Printf("Error SETEX value %v to %v, error %v", strings.Join(keyparts, ":"), value, err)
	} else {
		log.Printf("setting redis %+v = %+v", strings.Join(keyparts, ":"), value)
	}
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

func GetRedisGuidList(keyparts []string) []string {
	val := GetRedisStringValue(keyparts)
	result := []string{}
	if len(val) > 0 {
		runes := []rune(val)
		for i := 0; i < len(val)/36; i++ {
			subval := string(runes[i*36 : i*36+36])
			if !Contains(result, subval) {
				result = append(result, subval)
			}
		}
	}
	return result
}

func SetRedisKeyIfNotExists(keyparts []string) {
	ms := msp.Get()
	defer ms.Close()

	_, err := ms.Do("SETNX", strings.Join(keyparts, ":"), 1)
	if err != nil {
		log.Printf("Error SETNX value %v to %v, error %v", strings.Join(keyparts, ":"), 1, err)
	}
}

func SetRedisKeyTo0IfNotExists(keyparts []string) {
	ms := msp.Get()
	defer ms.Close()

	_, err := ms.Do("SETNX", strings.Join(keyparts, ":"), 0)
	if err != nil {
		log.Printf("Error SETNX value %v to %v, error %v", strings.Join(keyparts, ":"), 0, err)
	}
}

func IncrRedisValue(keyparts []string) { // no need to update expiration
	ms := msp.Get()
	defer ms.Close()

	_, err := ms.Do("INCR", strings.Join(keyparts, ":"))
	if err != nil {
		log.Printf("Error incrementing redis value %v, error %v", strings.Join(keyparts, ":"), err)
	}
}

func SetRedisKeyWithExpiration(keyparts []string) {
	SetRedisValueWithExpiration(keyparts, 1)
}

func GetRedisIntValue(keyparts []string) int {
	ms := msp.Get()
	defer ms.Close()
	value, err := redis.Int(ms.Do("GET", strings.Join(keyparts, ":")))
	if err != nil {
		// log.Printf("Error getting redis value %v, error %v", strings.Join(keyparts, ":"), err)
	}
	return value
}

func GetRedisIntValues(keys [][]string) []int {
	ms := msp.Get()
	defer ms.Close()

	formattedKeys := []string{}
	for _, key := range keys {
		formattedKeys = append(formattedKeys, strings.Join(key, ":"))
	}

	values, err := redis.Ints(ms.Do("MGET", formattedKeys[0], formattedKeys[1], formattedKeys[2], formattedKeys[3], formattedKeys[4]))
	if err != nil {
		log.Printf("Error getting redis values %v, error %v", formattedKeys, err)
	}
	return values
}

func GetRedisStringValue(keyparts []string) string {
	ms := msp.Get()
	defer ms.Close()
	value, err := redis.String(ms.Do("GET", strings.Join(keyparts, ":")))
	if err != nil {
		// log.Printf("Error getting redis value %v, error %v", strings.Join(keyparts, ":"), err)
	}
	return value
}

func GetRedisStringsValue(keyparts []string) []string {
	ms := msp.Get()
	defer ms.Close()
	value, err := redis.String(ms.Do("GET", strings.Join(keyparts, ":")))
	if err != nil {
		// log.Printf("Error getting redis value %v, error %v", strings.Join(keyparts, ":"), err)
	}
	if len(value) > 0 {
		return strings.Split(value, ",")
	}
	return []string{}
}
