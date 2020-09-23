package people360

import (
	"context"
	"regexp"
	"testing"
)

func TestPost(t *testing.T) {
	json := `[{"signature":{"ownerId":"wlm-rsa","source":"Admission","eventId":"a2a47ff8-2ec5-4198-a347-57da1119c754","eventType":"UPLOAD","fiberType":"default","recordId":"bfe733cd-1419-4be1-a61a-27e3bb419797","id":"864e4c56-b6b9-42c5-a92b-d582aad4ebe3"},"passthrough":{"ADCODE":"WLM1R0LQQQ","inputType":"P","masterProgramCode":"RHL","orderByDate":"2020-08-04","salutation":"To the Family of:","schoolCode":"WLM","schoolColor":"#37870e","schoolName":"Wilmington College","schoolYear":"2021","sponsorCode":"wlm-rsa"},"matchkeys":{"salutation":{"value":"","source":"","type":""},"nickname":{"value":"","source":"","type":""},"fname":{"value":"George","source":"Student name","type":""},"finitial":{"value":"G","source":"","type":""},"mname":{"value":"","source":"","type":""},"lname":{"value":"Scott","source":"Student name","type":""},"ad1":{"value":"777 Cherry Tree Rd","source":"Address","type":""},"ad1no":{"value":"777","source":"","type":""},"ad2":{"value":"Apt J149","source":"","type":""},"ad3":{"value":"","source":"","type":""},"city":{"value":"Upper Chichester","source":"City/State","type":""},"state":{"value":"PA","source":"","type":""},"zip":{"value":"19014-2470","source":"Zip Code","type":""},"zip5":{"value":"","source":"","type":""},"country":{"value":"US","source":"","type":""},"mailroute":{"value":"","source":"","type":""},"adtype":{"value":"Residential","source":"","type":""},"ziptype":{"value":"Standard","source":"","type":""},"recordtype":{"value":"H","source":"","type":""},"adbook":{"value":"Bill","source":"","type":""},"adparser":{"value":"smartystreet","source":"SS","type":""},"adcorrect":{"value":"TRUE","source":"","type":""},"advalid":{"value":"TRUE","source":"","type":""},"email":{"value":"test4@gmail.com","source":"Email","type":""},"phone":{"value":"","source":"","type":""},"trustedId":{"value":"","source":"","type":""},"clientId":{"value":"","source":"","type":""},"gender":{"value":"","source":"","type":""},"age":{"value":"","source":"","type":""},"dob":{"value":"","source":"","type":""},"organization":{"value":"WLM","source":"organization","type":""},"title":{"value":"","source":"","type":""},"role":{"value":"Student","source":"role","type":""},"status":{"value":"","source":"","type":""},"PermE":{"value":"","source":"","type":""},"PermM":{"value":"","source":"","type":""},"PermS":{"value":"","source":"","type":""}}}]`
	re := regexp.MustCompile(`\r?\n`)
	var message PubSubMessage
	message.Data = []byte(re.ReplaceAllString(json, ""))
	message.Attributes = map[string]string{"source": "test"}
	People360(context.Background(), message)
}
