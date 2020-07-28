package people360

import (
	"context"
	"regexp"
	"testing"
)

func TestPost(t *testing.T) {
	json := `[{"signature":{"ownerId":"wic-hrl","source":"Admission","eventId":"6ea2893a-05b0-42fa-915a-facf51234062","eventType":"UPLOAD","fiberType":"default","recordId":"f1f5a929-2cb0-43ed-94dc-9bf3c65ba6b9","id":"b5e406e5-64bd-4d25-ae78-6b317708617d"},"passthrough":{"ADCODE":"WIC1C0WQQ1","inputType":"P","masterProgramCode":"CWP","orderByDate":"2020-09-02","salutation":"To The Parent(s) of:","schoolCode":"WIC","schoolColor":"#ca2027","schoolName":"Wentworth Institute of Technology","schoolYear":"2021","sponsorCode":"wic-hrl"},"matchkeys":{"salutation":{"value":"","source":"","type":""},"nickname":{"value":"","source":"","type":""},"fname":{"value":"Justin","source":"﻿Student First Name","type":""},"finitial":{"value":"J","source":"","type":""},"mname":{"value":"","source":"","type":""},"lname":{"value":"Thomaszewski","source":"Student Last Name","type":""},"ad1":{"value":"19 Lincoln St","source":"Street Address 1","type":""},"ad1no":{"value":"19","source":"","type":""},"ad2":{"value":"","source":"","type":""},"ad3":{"value":"","source":"","type":""},"city":{"value":"Reading","source":"City","type":""},"state":{"value":"MA","source":"State","type":""},"zip":{"value":"01867-3132","source":"Zip Code","type":""},"zip5":{"value":"","source":"","type":""},"country":{"value":"US","source":"WM","type":""},"mailroute":{"value":"","source":"","type":""},"adtype":{"value":"Residential","source":"","type":""},"ziptype":{"value":"Standard","source":"","type":""},"recordtype":{"value":"S","source":"","type":""},"adbook":{"value":"Bill","source":"","type":""},"adparser":{"value":"smartystreet","source":"SS","type":""},"adcorrect":{"value":"FALSE","source":"","type":""},"advalid":{"value":"TRUE","source":"","type":""},"email":{"value":"thomaszewskij@wit.edu","source":"Student Email","type":""},"phone":{"value":"","source":"","type":""},"trustedId":{"value":"","source":"","type":""},"clientId":{"value":"","source":"","type":""},"gender":{"value":"","source":"","type":""},"age":{"value":"","source":"","type":""},"dob":{"value":"","source":"","type":""},"organization":{"value":"WIC","source":"organization","type":""},"title":{"value":"","source":"","type":""},"role":{"value":"Student","source":"role","type":""},"status":{"value":"","source":"","type":""},"PermE":{"value":"","source":"","type":""},"PermM":{"value":"","source":"","type":""},"PermS":{"value":"","source":"","type":""}}}]`
	re := regexp.MustCompile(`\r?\n`)
	var message PubSubMessage
	message.Data = []byte(re.ReplaceAllString(json, ""))
	message.Attributes = map[string]string{"source": "test"}
	People360(context.Background(), message)
}
