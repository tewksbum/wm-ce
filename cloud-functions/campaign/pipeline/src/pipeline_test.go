package pipeline

import (
	"encoding/json"
	"reflect"
	"testing"
)

var (
	campaignData = []byte(`{
		"Columns": [
			{
				"ERR": {
					"CampaignID": 0,
					"Name": 1,
					"Type": 0,
					"CampaignName": 1,
					"CampaignDescription": 0,
					"CampaignSize": 0,
					"CampaignColor": 0,
					"CampaignUnitPrice": 0,
					"CampaignContains": 0,
					"CampaignType": 0,
					"CampaignVendorId": 0,
					"CampaignVendor": 0,
					"CampaignCost": 0,
					"CampaignStars": 0,
					"CampaignCategory": 0,
					"CampaignMargin": 0
				},
				"Name": "pname",
				"Value": "A campaign NAME"
			}
		],
		"Owner": 108108108,
		"Request": "req",
		"Row": 1,
		"Source": "source",
		"TimeStamp": "2019-10-08"
	}`)
	outputRecord = &OutputRecord{
		Owner:     108108108,
		Request:   "req",
		Row:       1,
		Source:    "source",
		TimeStamp: "2019-10-08",
		// TrustedID: []OutputTrustedID{
		// 	OutputTrustedID{
		// 		Source:   "source",
		// 		SourceID: "108108108",
		// 	},
		// },
		Record: IdentifiedRecord{
			Name: "A campaign NAME",
		},
	}
)

func Test_pipelineParse(t *testing.T) {
	var input InputRecord
	if err := json.Unmarshal(campaignData, &input); err != nil {
		t.Fatal(err)
	}
	type args struct {
		input InputRecord
	}
	tests := []struct {
		name       string
		args       args
		wantOutput *OutputRecord
		wantErr    bool
	}{
		{
			name: "Pipeline Success",
			args: args{
				input: input,
			},
			wantOutput: outputRecord,
			wantErr:    false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotOutput, err := pipelineParse(tt.args.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("pipelineParse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotOutput, tt.wantOutput) {
				t.Errorf("pipelineParse() = %v, want %v", gotOutput, tt.wantOutput)
			}
		})
	}
}
