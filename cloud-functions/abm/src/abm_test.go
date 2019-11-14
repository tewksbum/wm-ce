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
	expectedjsonStr := `{"accessKey":"AccessKey","entityType":"EntityType","ownerId":1234,"eventId":"test"}`
	var outputHeader OutputHeader
	outputHeader.AccessKey = "AccessKey"
	outputHeader.OwnerID = 1234
	outputHeader.EntityType = "EntityType"
	output := Output{
		outputHeader,
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
		{Signature{123, "Source", "EventType", "EventId", "RecordId"}, "4dd32b1e279dbb92b338c08ef612956e"},
		{Signature{1234, "Source2", "EventType2", "EventId2", "RecordId2"}, "8a86193885f5bfac8046597384a0bbb1"},
	}
	for _, test := range tests {
		hash := getSignatureHash(test.input)
		if hash != test.expected {
			t.Errorf("Expecting %s got %s", test.expected, hash)
		}
	}
}
func Test360Unmarshalling(t *testing.T) {
	jsonstr := []byte(`{"id": "ae8fbaa1-cef5-4567-839a-8e3afa62bd3a","signature": {"ownerId": 2,"source": "Testing","eventId": "fdbca3e9-7e2e-4884-960a-7ecea5ee2aaf","eventType": "UPLOAD"},"signatures": [{"ownerId": 2,"source": "Testing","eventId": "fdbca3e9-7e2e-4884-960a-7ecea5ee2aaf","eventType": "UPLOAD","recordId": "ca82e9c7-9975-4a23-a52e-de9c466e3f77"}],"createdAt": "2019-11-11T20:43:07.697112481Z","timestamp": "2019-11-11T20:43:07.697111353Z","fibers": ["ca82e9c7-9975-4a23-a52e-de9c466e3f77"],"passthroughs": null,"matchKeys": [{"key": "ID","type": "","value": "","values": null}]}`)
	var request360 Request360
	if err := json.NewDecoder(bytes.NewBuffer(jsonstr)).Decode(&request360); err != nil {
		fmt.Printf("There was an issue decoding the message %v", string(jsonstr))
		return
	}
	// fmt.Printf("%v", request360)
}

func Test360PeopleOutput(t *testing.T) {
	jsonstr := []byte(`{"fake":"fake","id":"b4b0cfbf-f3fe-43a2-b39a-5f891c66b498","signature":{"ownerId":2,"source":"Testing","eventId":"f1ae31f5-069c-439b-ae9e-8ed7ba5ebfeb","eventType":"UPLOAD"},"signatures":[{"ownerId":2,"source":"Testing","eventId":"f1ae31f5-069c-439b-ae9e-8ed7ba5ebfeb","eventType":"UPLOAD","recordId":"0814836b-10fa-40d1-bd1c-a4228425abc9"}],"createdAt":"2019-11-12T19:07:13.298200252Z","timestamp":"2019-11-12T19:07:13.298199282Z","fibers":["0814836b-10fa-40d1-bd1c-a4228425abc9"],"passthroughs":null,"matchKeys":[{"key":"FNAME","type":"","value":"Omar","values":["Omar"]},{"key":"FINITIAL","type":"","value":"O","values":["O"]},{"key":"LNAME","type":"","value":"Acevedo","values":["Acevedo"]},{"key":"CITY","type":"","value":"North Richland Hills","values":["North Richland Hills"]},{"key":"STATE","type":"","value":"TX","values":["TX"]},{"key":"ZIP","type":"","value":"76180","values":["76180"]},{"key":"ZIP5","type":"","value":"76180","values":["76180"]},{"key":"COUNTRY","type":"","value":"","values":null},{"key":"EMAIL","type":"","value":"","values":null},{"key":"PHONE","type":"","value":"","values":null},{"key":"AD1","type":"","value":"4926 Maryanna Way","values":["4926 Maryanna Way"]},{"key":"AD2","type":"","value":"","values":null},{"key":"ADTYPE","type":"","value":"","values":null},{"key":"TRUSTEDID","type":"","value":"","values":null},{"key":"CLIENTID","type":"","value":"","values":null},{"key":"SALUTATION","type":"","value":"","values":null},{"key":"NICKNAME","type":"","value":"","values":null},{"key":"GENDER","type":"","value":"","values":null},{"key":"AGE","type":"","value":"","values":null},{"key":"DOB","type":"","value":"","values":null},{"key":"MAILROUTE","type":"","value":"","values":null},{"key":"ORGANIZATION","type":"","value":"","values":null},{"key":"TITLE","type":"","value":"2023","values":["2023"]},{"key":"ROLE","type":"","value":"","values":null},{"key":"STATUS","type":"","value":"","values":null}]}`)
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
	output := Output{
		outputHeader,
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
		LastName:     getFrom360Slice("LNAME", request360.MatchKeys).Value,
		Gender:       getFrom360Slice("GENDER", request360.MatchKeys).Value,
		Age:          getFrom360Slice("AGE", request360.MatchKeys).Value,
		Organization: getFrom360Slice("ORGANIZATION", request360.MatchKeys).Value,
		Title:        getFrom360Slice("TITLE", request360.MatchKeys).Value,
		Role:         getFrom360Slice("ROLE", request360.MatchKeys).Value,
	}
	output.People = &people
	output.OutputHeader.Signatures = getSignaturesHash(request360.Signatures)
	jsonStrOutput, err := json.Marshal(output)
	if err != nil {
		fmt.Printf("Couldn't parse it")
	}

	fmt.Printf("%v", string(jsonStrOutput))
}
