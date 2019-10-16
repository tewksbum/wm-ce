package orderpipeline

import (
	"encoding/json"
	"errors"
	"log"
)

// IdentifiedRecord struct
type IdentifiedRecord struct {
	TrustedID    string `json:"TrustedId" bigquery:"trustedid"`
	OrderID      string `json:"OrderId" bigquery:"orderid"`
	ConsigmentID string `json:"ConsigmentId" bigquery:"consigmentid"`
	ShipDate     string `json:"ShipDate" bigquery:"shipdate"`
}

// OutputRecord the output result
type OutputRecord struct {
	TrustedID []OutputTrustedID `json:"TrustedId"`
	Owner     int64             `json:"Owner"`
	Source    string            `json:"Source"`
	Request   string            `json:"Request"`
	Row       int               `json:"Row"`
	TimeStamp string            `json:"TimeStamp"`
	Record    IdentifiedRecord  `json:"Record" bigquery:"OrderConsignment"`
}

func pipelineParse(input InputRecord) (output *OutputRecord, err error) {
	var mkOutput IdentifiedRecord
	var trustedID string

	if err = parseOrderConsignment(&input, &mkOutput); err != nil {
		return nil, err
	}

	var Columns []InputColumn
	for _, column := range input.Columns {
		column.VER = getVER(&column)
		newColumn := column
		Columns = append(Columns, newColumn)
	}

	for _, column := range Columns {
		if column.ERR.TrustedID == 1 {
			trustedID = column.Value
		}
	}

	// assemble output
	output = new(OutputRecord)
	if trustedID != "" {
		outputTrustedID := OutputTrustedID{
			Source:   input.Source,
			SourceID: trustedID,
		}
		output.TrustedID = append(output.TrustedID, outputTrustedID)
	}
	output.Owner = input.Owner
	output.Request = input.Request
	output.Source = input.Source
	output.Row = input.Row
	output.TimeStamp = input.TimeStamp
	output.Record = mkOutput

	mkJSON, _ := json.Marshal(mkOutput)
	log.Printf("Columns after Clean up %v", string(mkJSON))

	return output, err
}

func parseOrderConsignment(input *InputRecord, mkOutput *IdentifiedRecord) (err error) {
	for _, column := range input.Columns {
		if column.ERR.TrustedID == 1 {
			mkOutput.OrderID = column.Value
		}
		if column.ERR.Order.Consignment.ID == 1 {
			mkOutput.ConsigmentID = column.Value
		}
		if column.ERR.Order.Consignment.ShipDate == 1 {
			mkOutput.ShipDate = column.Value
		}
	}
	if mkOutput.OrderID == "" {
		err = errors.New("[parseOrderConsignment]: OrderID not found")
	}
	if mkOutput.ConsigmentID == "" {
		err = errors.New("[parseOrderConsignment]: ConsignmentID not found")
	}
	return err
}
