package wemade

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"segment/utils"
	"time"

	"segment/datastore"
	"segment/utils/logger"

	"github.com/google/uuid"
)

var (
	errDecodingRequest      string = "Error decoding request %#v"
	errInternalErrorOcurred string = "Internal error occurred %#v"
	errInvalidAccessKey     string = "Invalid access key %#v"
	errAccountNotEnabled    string = "Account not enabled %#v"
	errStatusNoContent      string = "Method Options: No content"
)

// DecodeAPIInput serialize a json into a wemade.Request struct, checks the API key and
func DecodeAPIInput(projectID string, namespace string, body io.ReadCloser) (Record, error) {
	var input APIInput
	ctx := context.Background()
	requestID := uuid.New()
	if err := json.NewDecoder(body).Decode(&input); err != nil {
		return nil, logger.ErrFmt(errDecodingRequest, err)
	}

	dsClient, err := datastore.GetClient(&ctx, projectID)
	if err != nil {
		return nil, logger.ErrFmt(errInternalErrorOcurred, err)
	}
	query := datastore.QueryTableNamespace("Customer", namespace)
	query.Filter("AccessKey =", input.AccessKey).Limit(1)

	var entities []WMCustomer

	if _, err := dsClient.GetAll(ctx, query, &entities); err != nil {
		// fmt.Fprint(w, "{success: false, message: \"Internal error occurred, -2\"}")
		return nil, logger.ErrFmt(errInternalErrorOcurred, err)
	}
	if len(entities) == 0 {
		// fmt.Fprint(w, "{success: false, message: \"Invalid access key, -10\"}")
		return nil, logger.Err(fmt.Errorf(errInternalErrorOcurred, -10))
	}

	customer := entities[0]
	if customer.Enabled == false {
		// fmt.Fprint(w, "{success: false, message: \"Account is not enabled, -11\"}")
		return nil, logger.ErrStr(errAccountNotEnabled)
	}
	output := BaseRecord{
		RequestID:    requestID.String(),
		EntityType:   input.EntityType,
		CustomerID:   customer.Key.ID,
		Organization: customer.Name,
		Owner:        customer.Key.Name,
		Source:       input.Source,
		Passthrough:  utils.FlattenMap(input.Passthrough), // Do we need
		Attributes:   utils.FlattenMap(input.Attributes),  // these?
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
		return &output, nil
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
		return logger.ErrStr(errStatusNoContent)
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
