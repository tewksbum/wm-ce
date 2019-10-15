package orderpipeline

import "errors"

// Consignment detail
type Consignment struct {
	OrderID      string `json:"OrderId" bigquery:"orderid"`
	ConsigmentID string `json:"ConsigmentId" bigquery:"consigmentid"`
	ShipDate     string `json:"ShipDate" bigquery:"shipdate"`
}

// OrderConsignmentERR Entity Recognition
type OrderConsignmentERR struct {
	ID       int `json:"ID"`
	ShipDate int `json:"ShipDate"`
}

func parseOrderConsignment(input *InputRecord, mkOutput *IdentifiedRecord) (err error) {
	for _, column := range input.Columns {
		if column.ERR.Order.ID == 1 {
			mkOutput.Consignment.OrderID = column.Value
		}
		if column.ERR.Order.Consignment.ID == 1 {
			mkOutput.Consignment.ConsigmentID = column.Value
		}
		if column.ERR.Order.Consignment.ShipDate == 1 {
			mkOutput.Consignment.ShipDate = column.Value
		}
	}
	if mkOutput.Consignment.OrderID == "" {
		err = errors.New("[parseOrderConsignment]: OrderID not found")
	}
	if mkOutput.Consignment.ConsigmentID == "" {
		err = errors.New("[parseOrderConsignment]: ConsignmentID not found")
	}
	return err
}
