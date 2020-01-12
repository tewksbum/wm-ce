package wemade

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"time"

	"segment/db/datastore"
	"segment/models"
	"segment/utils"
	"segment/utils/logger"

	"github.com/google/uuid"
)

var (
	appEnv                = os.Getenv("APP_ENV")
	tempfixFixedAccesskey = os.Getenv("TEMP_FIXED_ACCESS_KEY")
)

// BuildInputFromData serializes the json into the APIInput struct and returns
func BuildInputFromData(data []byte) (APIInput, error) {
	var input APIInput

	err := json.Unmarshal(data, &input)
	if err != nil {
		return input, logger.ErrFmt(ErrDecodingRequest, err)
	}
	return input, nil
}

// BuildSweeperInputFromData serializes the json into the SweeperInput struct and returns
func BuildSweeperInputFromData(data []byte) (SweeperInput, error) {
	var input SweeperInput

	err := json.Unmarshal(data, &input)
	if err != nil {
		return input, logger.ErrFmt(ErrDecodingRequest, err)
	}
	return input, nil
}

// BuildRecordFromInput serialize a json into a Request struct, checks the API key and
func BuildRecordFromInput(projectID string, namespace string, data []byte, useFixedAccessKey bool) (models.Record, error) {
	ctx := context.Background()
	surrogateID := uuid.New().String()
	input, err := BuildInputFromData(data)
	if err != nil {
		return nil, err
	}
	writeToOwner := input.WriteToOwner
	ignoreUniqueFields := false
	accessKey := input.AccessKey
	logger.DebugFmt("RawInputJSON: %s", string(data))

	if tempfixFixedAccesskey != "" && useFixedAccessKey {
		// When using fixed access key also will ignore unique index
		logger.DebugFmt("Store for FixedAccessKey: %s", tempfixFixedAccesskey)
		accessKey = tempfixFixedAccesskey
		ignoreUniqueFields = true
	}

	logger.DebugFmt("Start Validating Customer: %s - %s -%s", projectID, namespace, accessKey)
	cust, err := validateCustomer(ctx, projectID, namespace, accessKey)
	logger.DebugFmt("Finished Validating Customer: %s - %s -%s", projectID, namespace, accessKey)
	if err != nil {
		return nil, err
	}
	owner := cust.Owner
	if owner == "" {
		owner = input.Owner
		if owner == "" {
			owner = cust.Name
		}
	}
	organization := input.Organization
	if organization == "" {
		organization = input.Attributes["organization"]
	}
	columns := models.DefaultColumnList
	selectColumns := input.Columns
	if len(selectColumns) < 1 {
		selectColumns = models.DefaultSelectColumnList
	}
	br := models.BaseRecord{
		IDField:          models.IDField,
		ColumnList:       columns,
		SelectColumnList: selectColumns,
		EntityType:       input.EntityType,
		OwnerID:          cust.Key.ID,
		Owner:            owner,
		Source:           input.Source,
		Passthrough:      input.Passthrough,
		Attributes:       utils.FlattenMap(input.Attributes),
		Timestamp:        time.Now(),
		AccessKey:        accessKey,
		WriteToOwner: writeToOwner,
	}

	// idata, _ := json.Marshal(input)
	// brdata, _ := json.Marshal(br)
	// logger.DebugFmt("APIInput: %s", string(idata))
	// logger.DebugFmt("BaseRecord: %s", string(brdata))

	entityType := strings.ToLower(input.EntityType)

	switch entityType {
	case models.TypeHousehold:
		br.DBopts = models.Options{
			Type:               models.CSQL,
			IgnoreUniqueFields: ignoreUniqueFields,
			Tablename:          models.TblHousehold,
			HasTablenameSuffix: true,
			TablenameSuffix:    br.GetStrOwnerID(),
			HasTablenamePrefix: true,
			TablenamePrefix:    models.TblnamePrefix,
			Filters:            input.Filters,
			// IsPartitioned:      true, PartitionField: models.DefPartitionField,
		}
		record := models.Household{}
		json.Unmarshal(data, &record)
		return &models.HouseholdRecord{
			SurrogateID: surrogateID,
			Signatures:  input.Signatures,
			BaseRecord:  br,
			Record:      record,
		}, nil
	case models.TypeEvent:
		br.DBopts = models.Options{
			Type:               models.CSQL,
			IgnoreUniqueFields: ignoreUniqueFields,
			Tablename:          models.TblEvent,
			HasTablenameSuffix: true,
			TablenameSuffix:    br.GetStrOwnerID(),
			HasTablenamePrefix: true,
			TablenamePrefix:    models.TblnamePrefix,
			Filters:            input.Filters,
			IsPartitioned:      true, PartitionField: models.DefPartitionField,
		}
		record := models.Event{}
		json.Unmarshal(data, &record)
		return &models.EventRecord{
			BaseRecord: br,
			Record:     record,
		}, nil
	case models.TypeProduct:
		br.DBopts = models.Options{
			Type:               models.CSQL,
			IgnoreUniqueFields: ignoreUniqueFields,
			Tablename:          models.TblProduct,
			HasTablenameSuffix: true,
			TablenameSuffix:    br.GetStrOwnerID(),
			HasTablenamePrefix: true,
			TablenamePrefix:    models.TblnamePrefix,
			Filters:            input.Filters,
			// IsPartitioned:      true, PartitionField: models.DefPartitionField,
		}
		record := models.Product{}
		json.Unmarshal(data, &record)
		return &models.ProductRecord{
			SurrogateID: surrogateID,
			BaseRecord:  br,
			Record:      record,
		}, nil
	case models.TypePeople:
		br.DBopts = models.Options{
			Type:               models.CSQL,
			IgnoreUniqueFields: ignoreUniqueFields,
			Tablename:          models.TblPeople,
			HasTablenameSuffix: true,
			TablenameSuffix:    br.GetStrOwnerID(),
			HasTablenamePrefix: true,
			TablenamePrefix:    models.TblnamePrefix,
			Filters:            input.Filters,
			// IsPartitioned:      true, PartitionField: models.DefPartitionField,
		}
		record := models.People{}
		record.Organization = organization
		json.Unmarshal(data, &record)
		return &models.PeopleRecord{
			BaseRecord:  br,
			Signatures:  input.Signatures,
			ExpiredSets: input.ExpiredSets,
			Record:      record,
		}, nil
	case models.TypeOrderHeader:
		br.DBopts = models.Options{
			Type:               models.CSQL,
			IgnoreUniqueFields: ignoreUniqueFields,
			Tablename:          models.TblOrderHeader,
			HasTablenameSuffix: true,
			TablenameSuffix:    br.GetStrOwnerID(),
			HasTablenamePrefix: true,
			TablenamePrefix:    models.TblnamePrefix,
			Filters:            input.Filters,
			// IsPartitioned:      true, PartitionField: models.DefPartitionField,
		}
		record := models.OrderHeader{}
		json.Unmarshal(data, &record)
		return &models.OrderHeaderRecord{
			SurrogateID: surrogateID,
			Signatures:  input.Signatures,
			BaseRecord:  br,
			Record:      record,
		}, nil
	case models.TypeOrderConsignment:
		br.DBopts = models.Options{
			Type:               models.CSQL,
			IgnoreUniqueFields: ignoreUniqueFields,
			Tablename:          models.TblOrderConsignment,
			HasTablenameSuffix: true,
			TablenameSuffix:    br.GetStrOwnerID(),
			HasTablenamePrefix: true,
			TablenamePrefix:    models.TblnamePrefix,
			Filters:            input.Filters,
			// IsPartitioned:      true, PartitionField: models.DefPartitionField,
		}
		record := models.OrderConsignment{}
		json.Unmarshal(data, &record)
		return &models.OrderConsignmentRecord{
			SurrogateID: surrogateID,
			Signatures:  input.Signatures,
			BaseRecord:  br,
			Record:      record,
		}, nil
	case models.TypeOrderDetail:
		br.DBopts = models.Options{
			Type:               models.CSQL,
			IgnoreUniqueFields: ignoreUniqueFields,
			Tablename:          models.TblOrderDetail,
			HasTablenameSuffix: true,
			TablenameSuffix:    br.GetStrOwnerID(),
			HasTablenamePrefix: true,
			TablenamePrefix:    models.TblnamePrefix,
			Filters:            input.Filters,
			// IsPartitioned:      true, PartitionField: models.DefPartitionField,
		}
		record := models.OrderDetail{}
		json.Unmarshal(data, &record)
		return &models.OrderDetailRecord{
			SurrogateID: surrogateID,
			Signatures:  input.Signatures,
			BaseRecord:  br,
			Record:      record,
		}, nil
	case models.TypeCampaign:
		br.DBopts = models.Options{
			Type:               models.CSQL,
			IgnoreUniqueFields: ignoreUniqueFields,
			Tablename:          models.TblCampaign,
			HasTablenameSuffix: true,
			TablenameSuffix:    br.GetStrOwnerID(),
			HasTablenamePrefix: true,
			TablenamePrefix:    models.TblnamePrefix,
			Filters:            input.Filters,
			// IsPartitioned:      true, PartitionField: models.DefPartitionField,
		}
		record := models.Campaign{}
		json.Unmarshal(data, &record)
		return &models.CampaignRecord{
			SurrogateID: surrogateID,
			BaseRecord:  br,
			Record:      record,
		}, nil
	case models.TypeDecode: // FCD table
		record := &models.DecodeRecord{}
		json.Unmarshal(data, record)
		record.BaseRecord = br
		record.IDField = models.DecodeIDField
		record.ColumnList = models.DecodeColumnList
		record.ColumnBlackList = models.DecodeBlackList
		record.DBopts = models.Options{
			Type:               models.CSQL,
			IgnoreUniqueFields: ignoreUniqueFields,
			Filters:            input.Filters,
			IsPartitioned:      false,
			HasTablenameSuffix: true,
			HasTablenamePrefix: true,
			TablenamePrefix:    models.TblnamePrefix,
			Tablename:          models.TblDecode,
			TablenameSuffix:    record.GetStrOwnerID(),
		}
		return record, nil
	default:
		br.DBopts = models.Options{
			Type:               models.CSQL,
			IgnoreUniqueFields: ignoreUniqueFields,
			Filters:            input.Filters,
			HasTablenameSuffix: true,
			HasTablenamePrefix: true,
			TablenamePrefix:    models.TblnamePrefix,
			Tablename:          models.TblShed,
			TablenameSuffix:    br.GetStrOwnerID(),
			// IsPartitioned:      true, PartitionField: models.DefPartitionField,
		}
		// the Shed - shabby werehouse where any dummy requests die in.
		br.EntityType = input.EntityType
		return &models.FallbackRecord{
			SurrogateID: surrogateID,
			BaseRecord:  br,
			Record:      models.FallbackData{Data: string(data)},
		}, nil
	}
}

func validateCustomer(ctx context.Context, pID string, ns string, aKey string) (DatastoreCustomer, error) {
	var c DatastoreCustomer
	dsClient, err := datastore.GetClient(&ctx, pID)
	if err != nil {
		return c, logger.ErrFmt(ErrInternalErrorOcurred, err)
	}
	query := datastore.QueryTablenamespace(models.DsTblCustomers, ns).
		Filter(models.DsFilterCustomers, aKey).Limit(1)

	var entities []DatastoreCustomer

	if _, err := dsClient.GetAll(ctx, query, &entities); err != nil {
		return c, logger.ErrFmt(ErrInternalErrorOcurred, err)
	}

	if len(entities) == 0 {
		return c, logger.ErrStr(ErrInvalidAccessKey)
	}

	c = entities[0]
	if c.Enabled == false {
		return c, logger.ErrStr(ErrAccountNotEnabled)
	}
	return c, nil
}
