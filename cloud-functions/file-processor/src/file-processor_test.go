package fileprocessor

import (
	"context"
	"regexp"
	"testing"
)

func TestFileProcessor(t *testing.T) {
	json := `{"signature":{"ownerId":"aam-sata","source":"MATCH","eventId":"a8991626-3bbc-418c-a3aa-ba5dffb57eb9","eventType":"UPLOAD"},"passthrough":{"ADCODE":"ATU1R0LQQQ","inputType":"New","listType":"Student","masterProgramCode":"RHL","orderByDate":"2020-08-03","salutation":"To The Parent(s) of:","schoolCode":"ATU","schoolName":"Arkansas Tech","schoolYear":"2021","sponsorCode":"atu-rha"},"attributes":{"PermE":"true","PermM":"true","PermS":"true","campaignId":"25196","campaignName":"atu-rha-RHL-2021","classStanding":"","listType":"Student","organization":"ATU","schoolYear":"2021","uploadType":"New"},"eventData":{"fileUrl":"https://storage.googleapis.com/tewks_test/1592323870-1592311951-SAQRHLPartial2021.xlsx","maxRows":0}}`
	re := regexp.MustCompile(`\r?\n`)
	var message PubSubMessage
	message.Data = []byte(re.ReplaceAllString(json, ""))
	ProcessFile(context.Background(), message)

}
