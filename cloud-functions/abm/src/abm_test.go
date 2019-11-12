package abm

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
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
func getOutputHash() string {
	var text string
	text = "diegoDiaz"
	hasher := md5.New()
	hasher.Write([]byte(text))
	return hex.EncodeToString(hasher.Sum(nil))
}
func Test360Unmarshalling(t *testing.T) {
	jsonstr := []byte(`{"id": "ae8fbaa1-cef5-4567-839a-8e3afa62bd3a","signature": {"ownerId": 2,"source": "Testing","eventId": "fdbca3e9-7e2e-4884-960a-7ecea5ee2aaf","eventType": "UPLOAD"},"signatures": [{"ownerId": 2,"source": "Testing","eventId": "fdbca3e9-7e2e-4884-960a-7ecea5ee2aaf","eventType": "UPLOAD","recordId": "ca82e9c7-9975-4a23-a52e-de9c466e3f77"}],"createdAt": "2019-11-11T20:43:07.697112481Z","timestamp": "2019-11-11T20:43:07.697111353Z","fibers": ["ca82e9c7-9975-4a23-a52e-de9c466e3f77"],"passthroughs": null,"matchKeys": [{"key": "ID","type": "","value": "","values": null}]}`)
	var request360 Request360
	if err := json.NewDecoder(bytes.NewBuffer(jsonstr)).Decode(&request360); err != nil {
		fmt.Printf("There was an issue decoding the message %v", string(jsonstr))
		return
	}
	fmt.Printf("%v", request360)
	fmt.Printf("%v", getOutputHash())
}
