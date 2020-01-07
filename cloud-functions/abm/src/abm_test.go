package abm

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"
)

func compareSlices(a, b []string) bool {
	// If one is nil, the other must also be nil.
	if (a == nil) != (b == nil) {
		return false
	}
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestAbmFlattening(t *testing.T) {
	expectedjsonStr := `{"accessKey":"AccessKey","entityType":"EntityType","ownerId":1234,"passthrough":null,"eventId":"test"}`
	var outputHeader OutputHeader
	outputHeader.AccessKey = "AccessKey"
	outputHeader.OwnerID = 1234
	outputHeader.EntityType = "EntityType"
	output := Output{
		outputHeader,
		Common{},
		&People{},
		&OrderDetail{},
		&OrderConsignment{},
		&OrderHeader{},
		&Product{},
		&Campaign{},
		&Event{},
		&Household{},
	}
	output.Event.EventID = "test"
	jsonStr, err := json.Marshal(output)
	if err != nil {
		fmt.Printf("Failed to unmarshall mixed struct  %v", err)
	}
	if string(jsonStr) != expectedjsonStr {
		t.Errorf("ABM output json not marshalling as expected %s got %s", expectedjsonStr, jsonStr)
	}
}
func TestSignatures(t *testing.T) {
	var tests = []struct {
		input    Signature
		expected string
	}{
		{Signature{"fake-sponsor", "Source", "EventType", "EventId", "RecordId"}, "cafd0d43416df66933400673cf25a6cc"},
		{Signature{"fake-sponsor", "Source2", "EventType2", "EventId2", "RecordId2"}, "5d949f9d5513d70187587746b2c8be4e"},
	}
	for _, test := range tests {
		hash := getSignatureHash(test.input)
		if hash != test.expected {
			t.Errorf("Expecting %s got %s", test.expected, hash)
		}
	}
}
func Test360Unmarshalling(t *testing.T) {
	jsonstr := []byte(`{"id": "ae8fbaa1-cef5-4567-839a-8e3afa62bd3a","signature": {"ownerId": "2","source": "Testing","eventId": "fdbca3e9-7e2e-4884-960a-7ecea5ee2aaf","eventType": "UPLOAD"},"signatures": [{"ownerId": "2","source": "Testing","eventId": "fdbca3e9-7e2e-4884-960a-7ecea5ee2aaf","eventType": "UPLOAD","recordId": "ca82e9c7-9975-4a23-a52e-de9c466e3f77"}],"createdAt": "2019-11-11T20:43:07.697112481Z","timestamp": "2019-11-11T20:43:07.697111353Z","fibers": ["ca82e9c7-9975-4a23-a52e-de9c466e3f77"],"passthroughs": null,"matchKeys": [{"key": "ID","type": "","value": "","values": null}]}`)
	var request360 Request360
	if err := json.NewDecoder(bytes.NewBuffer(jsonstr)).Decode(&request360); err != nil {
		fmt.Printf("There was an issue decoding the message %v", string(jsonstr))
		return
	}
	// fmt.Printf("%v", request360)
}

func Test360PeopleOutput(t *testing.T) {
	jsonstr := []byte(`{"id":"7b8fe145-a791-4ba5-808c-652cb26f8284","signature":{"ownerId":"ads-cec","source":"Admission","eventId":"1486e5d6-4bf8-4822-8858-ec7213042d54","eventType":"UPLOAD"},"signatures":[{"ownerId":"ads-cec","source":"Admission","eventId":"1486e5d6-4bf8-4822-8858-ec7213042d54","eventType":"UPLOAD","recordType":"","recordId":"46c0a074-748a-412e-ac5e-9fb373069ad7"}],"createdAt":"2020-01-02T16:43:20.281890635Z","timestamp":"0001-01-01T00:00:00Z","fibers":["2a627138-fb9b-48f8-be33-232db49839de"],"passthroughs":null,"matchKeys":[{"key":"LNAME","type":"","value":"Ralston","values":["Ralston"]},{"key":"CITY","type":"","value":"South Weymouth","values":["South Weymouth"]},{"key":"STATE","type":"","value":"MA","values":["MA"]},{"key":"ZIP","type":"","value":"02190-2334","values":["02190-2334"]},{"key":"ZIP5","type":"","value":"02190","values":["02190"]},{"key":"COUNTRY","type":"","value":"US","values":["US"]},{"key":"AD1","type":"","value":"62 Chauncy St","values":["62 Chauncy St"]},{"key":"AD1NO","type":"","value":"62","values":["62"]},{"key":"AD2","type":"","value":"","values":[]},{"key":"ADTYPE","type":"","value":"Residential","values":["Residential"]},{"key":"MAILROUTE","type":"","value":"","values":[]},{"key":"ADBOOK","type":"","value":"Residence","values":["Residence"]},{"key":"ADPARSER","type":"","value":"smartystreet","values":["smartystreet"]},{"key":"ADCORRECT","type":"","value":"FALSE","values":["FALSE"]},{"key":"ADVALID","type":"","value":"TRUE","values":["TRUE"]},{"key":"ZIPTYPE","type":"","value":"Standard","values":["Standard"]},{"key":"RECORDTYPE","type":"","value":"S","values":["S"]},{"key":"PermM","type":"","value":"","values":[]},{"key":"ORGANIZATION","type":"","value":"ADS","values":["ADS"]}]}`)
	var request360 Request360
	if err := json.NewDecoder(bytes.NewBuffer(jsonstr)).Decode(&request360); err != nil {
		fmt.Printf("There was an issue decoding the message %v %v", string(jsonstr), err)
		return
	}
	// fmt.Printf("%v", request360)
	var mkms []MatchKeyMap
	mkms = append(mkms, MatchKeyMap{
		MatchKey:   "fname",
		Source:     "FIRST_NAME",
		Type:       "",
		EntityType: "people"})
	// dynamicMap := fillMap(request360.MatchKeys, mkms)
	// fmt.Printf("%v", dynamicMap)
	// fmt.Printf("%v", People{}.Salutation)
	var outputHeader OutputHeader
	outputHeader.AccessKey = "accessKey"
	outputHeader.EntityType = "Inputtype"
	outputHeader.OwnerID = 2
	var common Common
	output := Output{
		outputHeader,
		common,
		&People{},
		&OrderDetail{},
		&OrderConsignment{},
		&OrderHeader{},
		&Product{},
		&Campaign{},
		&Event{},
		&Household{},
	}
	people := People{
		PeopleID:     request360.ID,
		Salutation:   getFrom360Slice("SALUTATION", request360.MatchKeys).Value,
		FirstName:    getFrom360Slice("FNAME", request360.MatchKeys).Value,
		Gender:       getFrom360Slice("GENDER", request360.MatchKeys).Value,
		Age:          getFrom360Slice("AGE", request360.MatchKeys).Value,
		Organization: getFrom360Slice("ORGANIZATION", request360.MatchKeys).Value,
		Title:        getFrom360Slice("TITLE", request360.MatchKeys).Value,
		Role:         getFrom360Slice("ROLE", request360.MatchKeys).Value,
	}
	output.People = &people
	output.People.Phones = []PhoneSt{
		PhoneSt{
			Phone: getFrom360Slice("PHONE", request360.MatchKeys).Value,
			Type:  getFrom360Slice("PHONE", request360.MatchKeys).Type,
		},
	}
	output.People.Emails = []EmailSt{
		EmailSt{
			Email: getFrom360Slice("EMAIL", request360.MatchKeys).Value,
			Type:  getFrom360Slice("EMAIL", request360.MatchKeys).Type,
		},
	}
	output.Common.LastName = getFrom360Slice("LNAME", request360.MatchKeys).Value
	output.Common.Signatures = getSignaturesHash(request360.Signatures)
	household := Household{
		Address1: getFrom360Slice("AD1", request360.MatchKeys).Value,
		Address2: getFrom360Slice("AD2", request360.MatchKeys).Value,
		Address3: getFrom360Slice("AD3", request360.MatchKeys).Value,
		City:     getFrom360Slice("CITY", request360.MatchKeys).Value,
		State:    getFrom360Slice("STATE", request360.MatchKeys).Value,
		Zip:      getFrom360Slice("ZIP", request360.MatchKeys).Value,
		Country:  getFrom360Slice("COUNTRY", request360.MatchKeys).Value,
	}
	output.Household = &household
	output.Common.PermE = getFrom360Slice("PermE", request360.MatchKeys).Value
	output.Common.PermM = getFrom360Slice("PermM", request360.MatchKeys).Value
	output.Common.PermS = getFrom360Slice("PermS", request360.MatchKeys).Value
	output.Common.ADTYPE = getFrom360Slice("ADTYPE", request360.MatchKeys).Value
	output.Common.ADBOOK = getFrom360Slice("ADBOOK", request360.MatchKeys).Value
	output.Common.ADPARSER = getFrom360Slice("ADPARSER", request360.MatchKeys).Value
	output.Common.ADCORRECT = getFrom360Slice("ADCORRECT", request360.MatchKeys).Value
	output.Common.ADVALID = getFrom360Slice("ADVALID", request360.MatchKeys).Value
	output.Common.ZIPTYPE = getFrom360Slice("ZIPTYPE", request360.MatchKeys).Value
	output.Common.RECORDTYPE = getFrom360Slice("RECORDTYPE", request360.MatchKeys).Value
	output.Common.DOB = getFrom360Slice("DOB", request360.MatchKeys).Value
	output.Common.STATUS = getFrom360Slice("STATUS", request360.MatchKeys).Value
	jsonStrOutput, err := json.Marshal(output)
	if err != nil {
		fmt.Printf("Couldn't parse %v", jsonStrOutput)
	}
	fmt.Printf("%v", string(jsonStrOutput))

}
func TestParsing(t *testing.T) {
	jsonstr := []byte(`{"id": "10bd2c2c-fc43-4bd8-ae0b-a3e739e4a744","signature":{"ownerId":"buv-bn","source":"OCM","eventId":"5d156436-3fe4-454d-95b6-ae0f65aae63c","eventType":"UPLOAD"},"signatures":[{"ownerId":"buv-bn","source":"OCM","eventId":"5d156436-3fe4-454d-95b6-ae0f65aae63c","eventType":"UPLOAD","recordId":"6623dc60-6fc3-4d58-a464-88e86c592322"}],"createdAt":"2019-12-02T13:29:30.703799715Z","fibers":["3cb35469-56c1-4a52-bc25-61d19f51a301"],"passthroughs":null,"matchKeys":[{"key":"SALUTATION","type":"","value":"","values":[]},{"key":"NICKNAME","type":"","value":"","values":[]},{"key":"FNAME","type":"","value":"Torres,Delilah","values":["Torres,Delilah"]},{"key":"FINITIAL","type":"","value":"T","values":["T"]},{"key":"LNAME","type":"","value":"","values":[]},{"key":"MNAME","type":"","value":"","values":[]},{"key":"AD1","type":"","value":"2450 N. LINDER AVE","values":["2450 N. LINDER AVE"]},{"key":"AD1","type":"","value":"2450","values":["2450"]},{"key":"AD2","type":"","value":"","values":[]},{"key":"AD3","type":"","value":"","values":[]},{"key":"CITY","type":"","value":"CHICAGO","values":["CHICAGO"]},{"key":"STATE","type":"","value":"IL","values":["IL"]},{"key":"ZIP","type":"","value":"60639","values":["60639"]},{"key":"ZIP5","type":"","value":"60639","values":["60639"]},{"key":"COUNTRY","type":"","value":"US","values":["US"]},{"key":"MAILROUTE","type":"","value":"","values":[]},{"key":"ADTYPE","type":"","value":"Home","values":["Home"]},{"key":"ADPARSER","type":"","value":"libpostal","values":["libpostal"]},{"key":"ADCORRECT","type":"","value":"","values":[]},{"key":"EMAIL","type":"","value":"","values":[]},{"key":"PHONE","type":"","value":"","values":[]},{"key":"TRUSTEDID","type":"","value":"","values":[]},{"key":"CLIENTID","type":"","value":"","values":[]},{"key":"GENDER","type":"","value":"","values":[]},{"key":"AGE","type":"","value":"","values":[]},{"key":"DOB","type":"","value":"","values":[]},{"key":"ORGANIZATION","type":"","value":"BUV","values":["BUV"]},{"key":"TITLE","type":"","value":"","values":[]},{"key":"ROLE","type":"","value":"","values":[]},{"key":"STATUS","type":"","value":"","values":[]}]}`)
	var request360 Request360
	if err := json.NewDecoder(bytes.NewBuffer(jsonstr)).Decode(&request360); err != nil {
		fmt.Printf("There was an issue decoding the message %v %v", string(jsonstr), err)
		return
	}
}

func TestEnvs(t *testing.T) {
	logDebug("Debug enabled")
}
