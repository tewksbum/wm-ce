package bq

import (
	"context"
	"segment/utils/logger"
	"strings"

	"cloud.google.com/go/bigquery"
)

// Options options for bigquery
type Options struct {
	IsPartitioned  bool
	PartitionField string
}

// Write writes the interface into BQ
func Write(projectID string, datasetID string, tableID string, opts Options, obj interface{}) error {
	ctx := context.Background()
	bqClient, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return logger.Err(err)
	}
	recordSchema, err := bigquery.InferSchema(obj)
	if err != nil {
		return logger.Err(err)
	}

	recordMetaData := &bigquery.TableMetadata{
		Schema: recordSchema,
	}
	if opts.IsPartitioned {
		recordMetaData.TimePartitioning = &bigquery.TimePartitioning{
			Field: opts.PartitionField,
		}
	}
	recordTableRef := bqClient.Dataset(datasetID).Table(tableID)
	if err := recordTableRef.Create(ctx, recordMetaData); err != nil {
		//Add update if error is 409
		if !strings.ContainsAny(err.Error(), "409") {
			return logger.Fatal(err)
		}
	}
	recordInserter := recordTableRef.Inserter()
	if err := recordInserter.Put(ctx, obj); err != nil {
		return logger.Err(err)
	}
	return nil
}
