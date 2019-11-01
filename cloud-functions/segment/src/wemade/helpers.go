package wemade

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"segment/utils"
	"time"

	"segment/datastore"
	"segment/utils/logger"

	"github.com/google/uuid"
)

// Error messages
var (
	ErrDecodingRequest      string = "Error decoding request %#v"
	ErrInternalErrorOcurred string = "Internal error occurred %#v"
	ErrInvalidAccessKey     string = "Invalid access key"
	ErrAccountNotEnabled    string = "Account not enabled"
	ErrStatusNoContent      string = "Method Options: No content"
)

// DecodeAPIInput serialize a json into a wemade.Request struct, checks the API key and
func DecodeAPIInput(projectID string, namespace string, body io.ReadCloser) (Record, error) {
	var input APIInput
	ctx := context.Background()
	requestID := uuid.New()
	if err := json.NewDecoder(body).Decode(&input); err != nil {
		return nil, logger.ErrFmt(ErrDecodingRequest, err)
	}
	logger.InfoFmt("input! : %#v", input.AccessKey)

	dsClient, err := datastore.GetClient(&ctx, projectID)
	if err != nil {
		return nil, logger.ErrFmt(ErrInternalErrorOcurred, err)
	}
	query := datastore.QueryTableNamespace("Customer", namespace)
	query.Filter("AccessKey = ", input.AccessKey).Limit(1)

	var entities []WMCustomer

	if _, err := dsClient.GetAll(ctx, query, &entities); err != nil {
		return nil, logger.ErrFmt(ErrInternalErrorOcurred, err)
	}

	if len(entities) == 0 {
		return nil, logger.ErrStr(ErrInvalidAccessKey)
	}

	customer := entities[0]
	if customer.Enabled == false {
		return nil, logger.ErrStr(ErrAccountNotEnabled)
	}

	output := BaseRecord{
		RequestID:    requestID.String(),
		EntityType:   input.EntityType,
		CustomerID:   customer.Key.ID,
		Organization: customer.Name,
		Owner:        customer.Key.Name,
		Source:       input.Source,
		Passthrough:  utils.FlattenMap(input.Passthrough),
		Attributes:   utils.FlattenMap(input.Attributes),
		Timestamp:    time.Now(),
	}
	// logger.InfoFmt("record: %+v", output)
	switch input.EntityType {
	case "event":
		return &EventRecord{
			BaseRecord: output,
			Data:       getEventFromDTO(input.Data),
		}, nil
	case "campaign":
		return &CampaignRecord{
			BaseRecord: output,
			Data:       getCampaignFromDTO(input.Data),
		}, nil
	case "product":
		return &ProductRecord{
			BaseRecord: output,
			Data:       getProductFromDTO(input.Data),
		}, nil
	case "people":
		return &PeopleRecord{
			BaseRecord: output,
			Data:       getPeopleFromDTO(input.Data),
		}, nil
	case "orderHeader":
		return &OrderHeaderRecord{
			BaseRecord: output,
			Data:       getOrderHeaderFromDTO(input.Data),
		}, nil
	case "orderConsignment":
		return &OrderConsignmentRecord{
			BaseRecord: output,
			Data:       getOrderConsignmentFromDTO(input.Data),
		}, nil
	case "orderDetail":
		return &OrderDetailRecord{
			BaseRecord: output,
			Data:       getOrderDetailFromDTO(input.Data),
		}, nil
	default:
		datab, _ := json.Marshal(input.Data)
		return &FallbackRecord{
			BaseRecord: output,
			Data:       string(datab),
		}, nil
	}
}

// SetHeaders sets the headers for the response
func SetHeaders(w http.ResponseWriter, r *http.Request) error {
	if r.Method == http.MethodOptions {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		w.Header().Set("Access-Control-Max-Age", "3600")
		w.WriteHeader(http.StatusNoContent)
		return logger.ErrStr(ErrStatusNoContent)
	}
	// Set CORS headers for the main request.
	w.Header().Set("Access-Control-Allow-Origin", "*")
	return nil
}

// Not exported funcs

func getEventFromDTO(data interface{}) Event {
	record := Event{}
	b, _ := json.Marshal(data)
	json.Unmarshal(b, &record)
	return record
}

func getCampaignFromDTO(data interface{}) Campaign {
	record := Campaign{}
	b, _ := json.Marshal(data)
	json.Unmarshal(b, &record)
	return record
}
func getProductFromDTO(data interface{}) Product {
	record := Product{}
	b, _ := json.Marshal(data)
	json.Unmarshal(b, &record)
	return record
}
func getOrderHeaderFromDTO(data interface{}) OrderHeader {
	record := OrderHeader{}
	b, _ := json.Marshal(data)
	json.Unmarshal(b, &record)
	return record
}
func getOrderConsignmentFromDTO(data interface{}) OrderConsignment {
	record := OrderConsignment{}
	b, _ := json.Marshal(data)
	json.Unmarshal(b, &record)
	return record
}
func getOrderDetailFromDTO(data interface{}) OrderDetail {
	record := OrderDetail{}
	b, _ := json.Marshal(data)
	json.Unmarshal(b, &record)
	return record
}

func getPeopleFromDTO(data interface{}) People {
	record := People{}
	b, _ := json.Marshal(data)
	json.Unmarshal(b, &record)
	return record
}
