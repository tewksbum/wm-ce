package orderpipeline

import (
	"encoding/json"
	"errors"
	"log"
)

// IdentifiedRecord struct
type IdentifiedRecord struct {
	TrustedID   string `json:"TrustedId" bigquery:"trustedid"`
	OrderID     string `json:"OrderId" bigquery:"orderid"`
	OrderNumber string `json:"OrderNumber" bigquery:"ordernumber"`
	CustomerID  string `json:"CustomerId" bigquery:"customerid"`
	OrderDate   string `json:"OrderDate" bigquery:"orderdate"`
	BillTo      string `json:"BillTo" bigquery:"billto"`
	Terms       string `json:"Terms" bigquery:"terms"`
	Total       string `json:"Total" bigquery:"total"`
}

// OutputRecord the output result
type OutputRecord struct {
	TrustedID []OutputTrustedID `json:"TrustedId"`
	Owner     int64             `json:"Owner"`
	Source    string            `json:"Source"`
	Request   string            `json:"Request"`
	Row       int               `json:"Row"`
	TimeStamp string            `json:"TimeStamp"`
	Record    IdentifiedRecord  `json:"Record" bigquery:"OrderHeader"`
}

func pipelineParse(input InputRecord) (output *OutputRecord, err error) {
	var mkOutput IdentifiedRecord
	var trustedID string

	if err = parseOrderHeader(&input, &mkOutput); err != nil {
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
		log.Printf("Column name: %v value: %v VER: %v ERR: %v", column.Name, column.Value, column.VER, column.ERR)
		if column.VER.IsTerms && len(mkOutput.Terms) == 0 && column.ERR.Order.Terms == 0 {
			mkOutput.Terms = column.Value
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

func parseOrderHeader(input *InputRecord, mkOutput *IdentifiedRecord) (err error) {
	for _, column := range input.Columns {
		if column.ERR.TrustedID == 1 {
			mkOutput.TrustedID = column.Value
		}
		if column.ERR.Order.ID == 1 {
			mkOutput.OrderID = column.Value
		}
		if column.ERR.Order.CustomerID == 1 {
			mkOutput.CustomerID = column.Value
		}
		if column.ERR.Order.OrderDate == 1 {
			mkOutput.OrderDate = column.Value
		}
		if column.ERR.Order.Number == 1 {
			mkOutput.OrderNumber = column.Value
		}
		if column.ERR.Order.Total == 1 {
			mkOutput.Total = column.Value
		}
		if column.ERR.Order.Terms == 1 {
			mkOutput.Terms = column.Value
		}
	}
	if mkOutput.TrustedID == "" {
		err = errors.New("[parseOrderHeader]: TrustedID not found")
	}
	return err
}
