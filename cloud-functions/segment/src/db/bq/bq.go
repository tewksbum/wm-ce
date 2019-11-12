package bq

import (
	"context"
	"fmt"
	"segment/models"
	"segment/utils"
	"segment/utils/logger"
	"segment/wemade"
	"strings"

	"cloud.google.com/go/bigquery"
)

// Write writes the interface into BQ
func Write(projectID string, datasetID string, tableID string,
	isPartitioned bool, partitionField string, obj interface{}) (updated bool, err error) {
	ctx := context.Background()
	bqClient, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return updated, logger.Err(err)
	}
	recordSchema, err := bigquery.InferSchema(obj)
	if err != nil {
		return updated, logger.Err(err)
	}
	dsMetadata := &bigquery.DatasetMetadata{
		Name: datasetID,
	}
	recordMetadata := &bigquery.TableMetadata{
		Schema: recordSchema,
	}
	if isPartitioned {
		recordMetadata.TimePartitioning = &bigquery.TimePartitioning{
			Field: partitionField,
		}
	}
	dset := bqClient.Dataset(datasetID)
	if err := dset.Create(ctx, dsMetadata); err != nil {
		if !strings.Contains(err.Error(), "duplicate") &&
			!strings.Contains(err.Error(), "409") {
			return updated, logger.Fatal(err)
		}
	}
	recordTableRef := dset.Table(tableID)
	if err := recordTableRef.Create(ctx, recordMetadata); err != nil {
		if !strings.Contains(err.Error(), "409") {
			return updated, logger.Fatal(err)
		}
	}
	recordInserter := recordTableRef.Inserter()
	if err := recordInserter.Put(ctx, obj); err != nil {
		return updated, logger.ErrFmt("[BQ.Write.recordInserter] %#v", err)
	}
	return updated, nil
}

func converInterfaceBQ(i interface{}) (o interface{}) {
	defer func() {
		if err := recover(); err != nil {
			logger.InfoFmt("recovering from: %q", err)
			o = i.(interface{})
		}
	}()
	o = *i.(*interface{})
	return o
}

// Read the interface from BQ
func Read(projectID string, datasetID string, tableID string, obj models.Record) (or wemade.OutputRecords, err error) {
	dbOpts := obj.GetDBOptions()
	params := []bigquery.QueryParameter{}
	querystr := "SELECT record.* from `" + projectID + "." + datasetID + "`." + tableID
	if len(dbOpts.Filters) > 0 {
		querystr += " WHERE "
		pfs, err := models.ParseFilters(dbOpts.Filters, true, "@", "record")
		if err != nil {
			return or, logger.ErrFmt("[bq.Read.ParsingFilters]: %#v", err)
		}
		for _, pf := range pfs {
			querystr += pf.ParsedCondition
			for i := 0; i < len(pf.ParamNames); i++ {
				v := converInterfaceBQ(pf.Values[i])
				switch t := v.(type) {
				case []interface{}:
					tmp := []string{}
					for _, vv := range v.([]interface{}) {
						tmp = append(tmp, fmt.Sprint(vv))
					}
					v = strings.Join(tmp, ",")
					logger.InfoFmt("param: %q - type: %T", v, t)
				default:
					logger.InfoFmt("param: %q - type: %T", v, t)
				}
				params = append(params, bigquery.QueryParameter{
					// Converting pfValues[i] which is interface{} to .(*interface{})
					// then assign Value the *value instead of the pointer.
					Name: pf.ParamNames[i], Value: v,
				})
			}
		}
		querystr += models.ParseOrderBy(dbOpts.Filters)
	}
	logger.InfoFmt("Query: %s\n%#v", querystr, params)
	ctx := context.Background()
	bqClient, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return or, logger.Err(err)
	}
	q := bqClient.Query(querystr)
	q.Parameters = params
	ri, err := q.Read(ctx)
	if err != nil {
		return or, logger.ErrFmt("[bq.Read.Query.Read]: %#v", err)
	}
	totalrows := int(ri.TotalRows)
	logger.InfoFmt("Total records: %d", totalrows)
	rec := models.GetRecordType(obj.GetEntityType())
	logger.InfoFmt("rec: %#v", rec)
	or.Count = totalrows
	for i := 1; i <= totalrows; i++ {
		ri.Next(rec)
		or.List = append(or.List, utils.StructToMap(rec, nil))
	}
	return or, err
}

// Delete the interfae from BQ
func Delete(projectID string, datasetID string, tableID string, r models.Record) error {
	for _, filter := range r.GetDBOptions().Filters {
		logger.InfoFmt("filter: %#v", filter)
	}
	return nil
}