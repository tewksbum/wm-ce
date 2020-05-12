package people360

import (
	"context"
	"regexp"
	"testing"
)

func TestPost(t *testing.T) {
	json := `[{"signature":{"ownerId":"abi-abim","source":"MATCH","eventId":"4d834e9e-40c7-4c02-bc77-1f7a5174da8c","eventType":"UPLOAD","fiberType":"default","recordId":"809f6182-1752-472b-a21a-0bb7aa4054cf","id":""},"passthrough":{"ADCODE":"ATU1R0LQQQ","inputType":"New","listType":"Student","masterProgramCode":"RHL","orderByDate":"2020-08-03","salutation":"To The Parent(s) of:","schoolCode":"ATU","schoolName":"Arkansas Tech","schoolYear":"2021","sponsorCode":"atu-rha"},"matchkeys":{"salutation":{"value":"","source":"","type":""},"nickname":{"value":"","source":"","type":""},"fname":{"value":"Tyler","source":"FIRSTNAME","type":""},"finitial":{"value":"T","source":"","type":""},"mname":{"value":"","source":"","type":""},"lname":{"value":"Arrington","source":"KEYNAME","type":""},"ad1":{"value":"308 Violet Aly","source":"ADDRESS LINE 1","type":"Residence"},"ad1no":{"value":"308","source":"","type":""},"ad2":{"value":"","source":"","type":""},"ad3":{"value":"","source":"","type":""},"city":{"value":"Grovetown","source":"CITY","type":""},"state":{"value":"GA","source":"STATE","type":""},"zip":{"value":"30813-0257","source":"POST CODE","type":""},"zip5":{"value":"","source":"","type":""},"country":{"value":"US","source":"WM","type":""},"mailroute":{"value":"","source":"","type":""},"adtype":{"value":"Residential","source":"ADDRESS LINE 1","type":""},"ziptype":{"value":"Standard","source":"","type":""},"recordtype":{"value":"S","source":"","type":""},"adbook":{"value":"Bill","source":"ADDRESS LINE 1","type":""},"adparser":{"value":"smartystreet","source":"SS","type":""},"adcorrect":{"value":"FALSE","source":"","type":""},"advalid":{"value":"TRUE","source":"","type":""},"email":{"value":"a.arrington02@comcast.net","source":"RELATED EMAIL ADDRESS","type":"Private"},"phone":{"value":"","source":"","type":""},"trustedId":{"value":"","source":"","type":""},"clientId":{"value":"","source":"","type":""},"gender":{"value":"","source":"","type":""},"age":{"value":"","source":"","type":""},"dob":{"value":"","source":"","type":""},"organization":{"value":"ATU","source":"organization","type":""},"title":{"value":"","source":"","type":""},"role":{"value":"","source":"","type":""},"status":{"value":"","source":"","type":""},"PermE":{"value":"","source":"","type":""},"PermM":{"value":"","source":"","type":""},"PermS":{"value":"","source":"","type":""}}}]`
	re := regexp.MustCompile(`\r?\n`)
	var message PubSubMessage
	message.Data = []byte(re.ReplaceAllString(json, ""))
	message.Attributes = map[string]string{"source": "test"}
	People360(context.Background(), message)
}
