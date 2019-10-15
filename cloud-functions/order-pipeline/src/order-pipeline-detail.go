package orderpipeline

import "errors"

// Detail of the order
type Detail struct {
	DetailID     string `json:"DetailId" bigquery:"detailid"`
	OrderID      string `json:"OrderId" bigquery:"orderid"`
	ConsigmentID string `json:"ConsigmentId" bigquery:"consigmentid"`
	ProductID    string `json:"ProductId" bigquery:"productid"`
	SKU          string `json:"SKU" bigquery:"sku"`
	UPC          string `json:"UPC" bigquery:"upc"`
}

// OrderDetailERR Entity Recognition
type OrderDetailERR struct {
	ID           int `json:"ID"`
	OrderID      int `json:"OrderID"`
	ConsigmentID int `json:"ConsigmentID"`
	ProductID    int `json:"ProductID"`
	ProductSKU   int `json:"ProductSKU"`
	ProductUPC   int `json:"ProductUPC"`
}

func parseOrderDetail(input *InputRecord, mkOutput *IdentifiedRecord) (err error) {
	for _, column := range input.Columns {
		if column.ERR.Order.ID == 1 {
			mkOutput.Detail.OrderID = column.Value
		}
		if column.ERR.Order.Detail.ID == 1 {
			mkOutput.Detail.DetailID = column.Value
		}
		if column.ERR.Order.CustomerID == 1 {
			mkOutput.CustomerID = column.Value
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
