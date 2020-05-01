package filereport

import "time"

// FileReport holds all elements on a list file
type FileReport struct {
	ID                 string                 `json:"id,omitempty"`
	RequestedAt        time.Time              `json:"requestedAt,omitempty"`
	ProcessingBegin    time.Time              `json:"processingBegin,omitempty"`
	ProcessingEnd      time.Time              `json:"processingEnd,omitempty"`
	Attributes         []NameValue            `json:"attributes,omitempty"`
	Passthroughs       []NameValue            `json:"passthroughs,omitempty"`
	CustomerID         string                 `json:"customerId,omitempty"`
	InputFilePath      string                 `json:"inputFilePath,omitempty"`
	InputFileName      string                 `json:"inputFileName,omitempty"`
	Owner              string                 `json:"owner,omitempty"`
	StatusLabel        string                 `json:"statusLabel,omitempty"`
	StatusBy           string                 `json:"statusBy,omitempty"`
	StatusTime         time.Time              `json:"statusTime,omitempty"`
	Errors             []ReportError          `json:"errors"`
	Warnings           []ReportError          `json:"warnings"`
	Counters           []ReportCounter        `json:"counters,omitempty"`      // this is for input
	Counts             map[string]interface{} `json:"counts"`                  // this is for elastic
	Columns            []string               `json:"columns,omitempty"`       // this is for input, file processor to write this before streaming
	ColumnMaps         []NameValue            `json:"map,omitempty"`           // this is for input
	Mapping            map[string]interface{} `json:"mapping,omitempty"`       // this is for elastic
	InputStatistics    map[string]ColumnStat  `json:"inputStats,omitempty"`    // this is for input, file processor to write this after streaming
	MatchKeyStatistics map[string]ColumnStat  `json:"matchKeyStats,omitempty"` // this is for input, file processor to write this after streaming
	StatusHistory      []ReportStatus         `json:"history"`
	//OutputStatistics []ReportStat           `json:"outputStats,omitempty"`
}

// ColumnStat stores input data statistics
type ColumnStat struct {
	Name     string  `json:"-"`
	Min      string  `json:"min"`
	Max      string  `json:"max"`
	Sparsity float32 `json:"sparsity"`
}

// ReportError stores errors and warnings
type ReportError struct {
	FileLevel bool   `json:"file_level,omitempty"`
	Row       int    `json:"row,omitempty"`
	RecordID  string `json:"record_id,omitempty"`
	Field     string `json:"field,omitempty"`
	Value     string `json:"value,omitempty"`
	Message   string `json:"message,omitempty"`
}

// ReportCounter stores record, purge, murge
type ReportCounter struct {
	Type      string `json:"type,omitempty"`
	Name      string `json:"name,omitempty"`
	Count     int    `json:"count,omitempty"`
	Increment bool   `json:"inc,omitempty"`
}

// ReportStat stores metric such as sparsity
type ReportStat struct {
	Field    string `json:"field,omitempty"`
	Metric   string `json:"metric,omitempty"`
	MinValue string `json:"minval,omitempty"`
	MaxValue string `json:"maxval,omitempty"`
	Value    int    `json:"value,omitempty"`
}

// ReportStatus stores history
type ReportStatus struct {
	Label     string    `json:"label,omitempty"`
	Timestamp time.Time `json:"ts,omitempty"`
	Function  string    `json:"function,omitempty"`
}

// NameValue stores name value pair
type NameValue struct {
	Name  string `json:"k,omitempty"`
	Value string `json:"v,omitempty"`
}
