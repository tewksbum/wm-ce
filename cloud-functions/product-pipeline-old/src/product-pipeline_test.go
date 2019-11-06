package productpipeline

import (
	"encoding/json"
	"reflect"
	"testing"
)

var (
	productData = []byte(`{
		"Columns": [
			{
				"ERR": {
					"ProductID": 0,
					"ProductSKU": 0,
					"ProductUPC": 0,
					"ProductName": 1,
					"ProductDescription": 0,
					"ProductSize": 0,
					"ProductColor": 0,
					"ProductUnitPrice": 0,
					"ProductContains": 0,
					"ProductType": 0,
					"ProductVendorId": 0,
					"ProductVendor": 0,
					"ProductCost": 0,
					"ProductStars": 0,
					"ProductCategory": 0,
					"ProductMargin": 0
				},
				"Name": "pname",
				"Value": "A product NAME"
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
			Name: "A product NAME",
		},
	}
)

func Test_pipelineParse(t *testing.T) {
	var input InputRecord
	if err := json.Unmarshal(productData, &input); err != nil {
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

func Test_writeToBQ(t *testing.T) {
	type args struct {
		datasetID string
		tableID   string
		output    *OutputRecord
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "Write To BQ a Product with just Name",
			args: args{
				datasetID: "108108108",
				tableID:   "test_products",
				output:    outputRecord,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := writeToBQ(tt.args.datasetID, tt.args.tableID, tt.args.output); (err != nil) != tt.wantErr {
				t.Errorf("writeToBQ() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
