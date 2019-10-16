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
	DetailID     string `json:"DetailId" bigquery:"detailid"`
	ConsigmentID string `json:"ConsigmentId" bigquery:"consigmentid"`
	ProductID    string `json:"ProductId" bigquery:"productid"`
	SKU          string `json:"SKU" bigquery:"sku"`
	UPC          string `json:"UPC" bigquery:"upc"`
}

// OutputRecord the output result
type OutputRecord struct {
	TrustedID []OutputTrustedID `json:"TrustedId"`
	Owner     int64             `json:"Owner"`
	Source    string            `json:"Source"`
	Request   string            `json:"Request"`
	Row       int               `json:"Row"`
	TimeStamp string            `json:"TimeStamp"`
	Record    IdentifiedRecord  `json:"Record" bigquery:"OrderDetail"`
}

func pipelineParse(input InputRecord) (output *OutputRecord, err error) {
	var mkOutput IdentifiedRecord
	var trustedID string

	if err = parseOrderDetail(&input, &mkOutput); err != nil {
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

func parseOrderDetail(input *InputRecord, mkOutput *IdentifiedRecord) (err error) {
	for _, column := range input.Columns {
		if column.ERR.TrustedID == 1 {
			mkOutput.OrderID = column.Value
		}
		if column.ERR.Order.Detail.ID == 1 {
			mkOutput.DetailID = column.Value
		}
		if column.ERR.Order.Detail.ConsigmentID == 1 {
			mkOutput.ConsigmentID = column.Value
		}
		if column.ERR.Order.Detail.ProductID == 1 {
			mkOutput.ProductID = column.Value
		}
		if column.ERR.Order.Detail.ProductSKU == 1 {
			mkOutput.SKU = column.Value
		}
		if column.ERR.Order.Detail.ProductUPC == 1 {
			mkOutput.UPC = column.Value
		}
	}
	if mkOutput.OrderID == "" {
		err = errors.New("[parseOrderDetail]: OrderID not found")
	}
	if mkOutput.DetailID == "" {
		err = errors.New("[parseOrderDetail]: OrderDetailID not found")
	}
	return err
}
