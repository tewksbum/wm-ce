package wemade

import (
	"context"
	"encoding/json"
	"io"
	"segment/bq"
	"strings"
	"time"

	"segment/datastore"
	"segment/utils"
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

// Table names
var (
	tblEvent            string = "events"
	tblOrderHeader      string = "orderheaders"
	tblOrderDetail      string = "orderdetails"
	tblOrderConsignment string = "orderconsignments"
	tblHousehold        string = "households"
	tblProduct          string = "products"
	tblPeople           string = "people"
	tblShed             string = "shed"
	// tblCampaign         string = "campaign"
	defPartitionField string = "timestamp"
	dstblCustomers    string = "Customer"
	dsfilterCustomers string = "AccessKey = "
)

// DecodeAPIInput serialize a json into a wemade.Request struct, checks the API key and
func DecodeAPIInput(projectID string, namespace string, body io.ReadCloser) (Record, error) {
	var input APIInput
	ctx := context.Background()
	surrogateID := uuid.New()
	if err := json.NewDecoder(body).Decode(&input); err != nil {
		return nil, logger.ErrFmt(ErrDecodingRequest, err)
	}

	dsClient, err := datastore.GetClient(&ctx, projectID)
	if err != nil {
		return nil, logger.ErrFmt(ErrInternalErrorOcurred, err)
	}
	query := datastore.QueryTableNamespace(dstblCustomers, namespace)
	query.Filter(dsfilterCustomers, input.AccessKey).Limit(1)

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
		SurrogateID: surrogateID.String(),
		EntityType:  input.EntityType,
		CustomerID:  customer.Key.ID,
		Owner:       input.Owner,
		Source:      input.Source,
		Passthrough: utils.FlattenMap(input.Passthrough),
		Attributes:  utils.FlattenMap(input.Attributes),
		Timestamp:   time.Now(),
	}

	entityType := strings.ToLower(input.EntityType)
	b, _ := json.Marshal(input.Data)

	switch entityType {
	case tblHousehold:
		output.bqOpts = bq.Options{IsPartitioned: true, PartitionField: defPartitionField}
		record := Household{}
		json.Unmarshal(b, &record)
		return &HouseholdRecord{
			BaseRecord: output,
			Record:     record,
		}, nil
	case tblEvent:
		output.bqOpts = bq.Options{IsPartitioned: true, PartitionField: defPartitionField}
		record := Event{}
		json.Unmarshal(b, &record)
		return &EventRecord{
			BaseRecord: output,
			Record:     record,
		}, nil
	case tblProduct:
		record := Product{}
		json.Unmarshal(b, &record)
		output.bqOpts = bq.Options{IsPartitioned: false}
		return &ProductRecord{
			BaseRecord: output,
			Record:     record,
		}, nil
	case tblPeople:
		output.bqOpts = bq.Options{IsPartitioned: false}
		record := People{}
		json.Unmarshal(b, &record)
		return &PeopleRecord{
			BaseRecord: output,
			Record:     record,
		}, nil
	case tblOrderHeader:
		output.bqOpts = bq.Options{IsPartitioned: false}
		record := OrderHeader{}
		json.Unmarshal(b, &record)
		return &OrderHeaderRecord{
			BaseRecord: output,
			Record:     record,
		}, nil
	case tblOrderConsignment:
		output.bqOpts = bq.Options{IsPartitioned: false}
		record := OrderConsignment{}
		json.Unmarshal(b, &record)
		return &OrderConsignmentRecord{
			BaseRecord: output,
			Record:     record,
		}, nil
	case tblOrderDetail:
		output.bqOpts = bq.Options{IsPartitioned: false}
		record := OrderDetail{}
		json.Unmarshal(b, &record)
		return &OrderDetailRecord{
			BaseRecord: output,
			Record:     record,
		}, nil
	// case tblCampaign:
	//  output.bqOpts = bq.Options{IsPartitioned: false}
	//  record := Campaign{}
	//  json.Unmarshal(b, &record)
	// 	return &CampaignRecord{
	// 		BaseRecord: output,
	// 		Record:     record,
	// 	}, nil
	default:
		dbyte, _ := json.Marshal(input.Data)
		output.bqOpts = bq.Options{IsPartitioned: false}
		// the Shed - shabby werehouse where any dummy requests die in.
		output.EntityType = tblShed
		return &FallbackRecord{
			BaseRecord: output,
			Record:     FallbackData{Data: string(dbyte)},
		}, nil
	}
}
