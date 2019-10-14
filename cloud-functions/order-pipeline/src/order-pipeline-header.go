package orderpipeline

import (
	"errors"
)

// OrderERR Entity Recognition
type OrderERR struct {
	//  Trusted ID
	ID     int `json:"ID"`
	Number int `json:"Number"`
	//  Header
	CustomerID int `json:"CustomerID"`
	OrderDate  int `json:"OrderDate"`
	Terms      int `json:"Terms"`
	Total      int `json:"Total"`
	BillTo     int `json:"BillTo"`
	//  Consignment
	Consignment OrderConsignmentERR `json:"Consignment"`
	//  Detail
	Detail OrderDetailERR `json:"Detail"`
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
	if mkOutput.OrderID == "" {
		err = errors.New("[parseOrderHeader]: OrderID not found")
	}
	return err
}
