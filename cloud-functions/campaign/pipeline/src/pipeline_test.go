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
					"TrustedID":0,
					"CampaignId":0,
					"Name":1,
					"Type":0,
					"Channel":0,
					"StartDate":0,
					"EndDate":0,
					"Budget":0
				},
				"Name": "pname",
				"Value": "A campaign NAME"
			},
			{
				"ERR": {
					"TrustedID":0,
					"CampaignId":0,
					"Name":0,
					"Type":0,
					"Channel":0,
					"StartDate":0,
					"EndDate":0,
					"Budget":1
				},
				"Name": "budget",
				"Value": "108108"
			}
		],
		"Owner": 108108108,
		"Request": "857e551e-5f36-47ee-9699-f04fed5756e6",
		"Row": 1,
		"Source": "source",
		"TimeStamp": "2019-10-22"
	}`)
	outputRecord = &OutputRecord{
		Owner:     108108108,
		Request:   "857e551e-5f36-47ee-9699-f04fed5756e6",
		Row:       1,
		Source:    "source",
		TimeStamp: "2019-10-22",
		// TrustedID: []OutputTrustedID{
		// 	OutputTrustedID{
		// 		Source:   "source",
		// 		SourceID: "108108108",
		// 	},
		// },
		Record: IdentifiedRecord{
			Name:   "A campaign NAME",
			Budget: "108108",
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
				t.Errorf("pipelineParse() = \n%v, want \n%v", gotOutput, tt.wantOutput)
			}
		})
	}
}
