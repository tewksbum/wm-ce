package fileprocessor

import (
	"context"
	"regexp"
	"testing"
)

func TestFileProcessor(t *testing.T) {
	json := `{"signature":{"ownerId":"wsc-rlh","source":"Admission","eventId":"0ab2aadd-61b5-49b8-b013-06dd36f2ab03","eventType":"UPLOAD"},"passthrough":{"ADCODE":"WSC1R0LQQQ","inputType":"P","listType":"Student","masterProgramCode":"RHL","orderByDate":"2020-08-21","salutation":"To The Parent(s) of:","schoolCode":"WSC","schoolColor":"#071779","schoolName":"Worcester State University","schoolYear":"2021","sponsorCode":"wsc-rlh"},"attributes":{"PermE":"true","PermM":"true","PermS":"true","campaignId":"25635","campaignName":"wsc-rlh-RHL-2021","classStanding":"All Frosh","listType":"Student","organization":"WSC","schoolYear":"2021","uploadType":"P"},"eventData":{"fileUrl":"https://storage.googleapis.com/qa-ocm-portal/uploads/student-lists/wsc-rlh/1590421802-1589814042-WSCRHLAddl2021%20%282%29.xlsx?Expires=1905781802\u0026GoogleAccessId=portal-gql%40ocm-core-qa.iam.gserviceaccount.com\u0026Signature=qnIGNtG%2BFO5LtS5xzsWXDGFGfdTCPSZA2Xc429vP1kagHYsQchaIuYOO7rVmFNlJlUM6fT6wSGf86iOfJ7jqvhl1PnU6z2ouyPQSyLQiq1H1CjaU1XD0Dy6AVmDwycMFwrcKcdT7kYnRHRq8z26hpybUJf%2BeJHBZfo2Vlzu24XxH9LzbJh2spI8wn8E2vM2BUCYG%2FYl0DKEq%2B0YW9fxr1XgkKVHoJSTtH4N2Yfn7FDq6eOZm8iBFFZ7EoT18AFb7GbwBTndAplUZAdLSHu63CM45TkgzIishC%2BIaIXClBCuPj1brwBM%2BJynvWP%2B4%2FnhQGuQe35bstNilBEikW599Kw%3D%3D","maxRows":0}}`
	re := regexp.MustCompile(`\r?\n`)
	var message PubSubMessage
	message.Data = []byte(re.ReplaceAllString(json, ""))
	ProcessFile(context.Background(), message)

}
