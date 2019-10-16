package orderpipeline

import (
	"encoding/json"
	"errors"
	"log"
)

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
			mkOutput.Detail.DetailID = column.Value
		}
		if column.ERR.Order.Detail.ConsigmentID == 1 {
			mkOutput.Detail.ConsigmentID = column.Value
		}
		if column.ERR.Order.Detail.ProductID == 1 {
			mkOutput.Detail.ProductID = column.Value
		}
		if column.ERR.Order.Detail.ProductSKU == 1 {
			mkOutput.Detail.SKU = column.Value
		}
		if column.ERR.Order.Detail.ProductUPC == 1 {
			mkOutput.Detail.UPC = column.Value
		}
	}
	if mkOutput.OrderID == "" {
		err = errors.New("[parseOrderDetail]: OrderID not found")
	}
	if mkOutput.Detail.DetailID == "" {
		err = errors.New("[parseOrderDetail]: OrderDetailID not found")
	}
	return err
}
