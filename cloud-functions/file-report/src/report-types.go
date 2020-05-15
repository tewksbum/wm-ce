package filereport

import "time"

// FileReport holds all elements on a list file
type FileReport struct {
	ID                 string                `json:"id,omitempty"`
	RequestedAt        time.Time             `json:"requestedAt,omitempty"`
	ProcessingBegin    time.Time             `json:"processingBegin,omitempty"`
	ProcessingEnd      time.Time             `json:"processingEnd,omitempty"`
	Attributes         []NameValue           `json:"attributes,omitempty"`
	Passthroughs       []NameValue           `json:"passthroughs,omitempty"`
	CustomerID         string                `json:"customerId,omitempty"`
	InputFilePath      string                `json:"inputFilePath,omitempty"`
	InputFileName      string                `json:"inputFileName,omitempty"`
	Owner              string                `json:"owner,omitempty"`
	StatusLabel        string                `json:"statusLabel,omitempty"`
	StatusBy           string                `json:"statusBy,omitempty"`
	StatusTime         time.Time             `json:"statusTime,omitempty"`
	Audits             []ReportError         `json:"audits"`
	Errors             []ReportError         `json:"errors"`
	Warnings           []ReportError         `json:"warnings"`
	Counters           []ReportCounter       `json:"counters,omitempty"`   // this is for input
	Counts             []CounterGroup        `json:"counts"`               // this is for elastic
	Columns            []string              `json:"columns,omitempty"`    // this is for input, file processor to write this before streaming
	ColumnMaps         []NameValue           `json:"map,omitempty"`        // this is for input
	Mapping            []NameMappedCounter   `json:"mapping,omitempty"`    // this is for elastic
	InputStatistics    map[string]ColumnStat `json:"inputStats,omitempty"` // this is for input, file processor to write this after streaming
	ColumnStats        []ColumnStat          `json:"fields,omitempty"`     // this is for elastic
	MatchKeyStatistics map[string]int        `json:"matchKeyStats"`        // this is for input
	MatchKeyCounts     []KeyCounter          `json:"matchKeyCounts"`       // this is for elastic
	StatusHistory      []ReportStatus        `json:"history"`
	RecordList         []RecordDetail        `json:"recordList,omitempty"`
	FiberList          []FiberDetail         `json:"fiberList,omitempty"`
	SetList            []SetDetail           `json:"setList,omitempty"`
	Records            []RecordDetail        `json:"records"`
	Fibers             []FiberDetail         `json:"fibers"`
	Sets               []SetDetail           `json:"sets"`
}

// CounterGroup counter groups
type CounterGroup struct {
	Group string       `json:"group"`
	Items []KeyCounter `json:"items"`
}

// RecordDetail stores detail about a record
type RecordDetail struct {
	ID          string    `json:"id,omitempty"`
	RowNumber   int       `json:"row,omitempty"`
	CreatedOn   time.Time `json:"createdOn,omitempty"`
	IsPerson    string    `json:"isPerson,omitempty"`
	Disposition string    `json:"disposition,omitempty"`
	Fibers      []string  `json:"fibers"`
}

// FiberDetail stores detail about a record
type FiberDetail struct {
	ID          string    `json:"id,omitempty"`
	CreatedOn   time.Time `json:"createdOn,omitempty"`
	Type        string    `json:"type,omitempty"`
	Disposition string    `json:"disposition,omitempty"`
	Sets        []string  `json:"sets"`
}

// SetDetail stores detail about a record
type SetDetail struct {
	ID         string    `json:"id,omitempty"`
	FiberCount int       `json:"fiberCount,omitempty"`
	CreatedOn  time.Time `json:"createdOn,omitempty"`
	DeletedOn  time.Time `json:"deletedOn,omitempty"`
	IsDeleted  bool      `json:"isDeleted,omitempty"`
	ReplacedBy string    `json:"replacedBy,omitempty"`
}

// ColumnStat stores input data statistics
type ColumnStat struct {
	Name     string       `json:"name"`
	Min      string       `json:"min"`
	Max      string       `json:"max"`
	Sparsity float32      `json:"sparsity"`
	Mapped   []MapCounter `json:"mapped"`
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

// KeyCounter stores name value pair
type KeyCounter struct {
	Key   string `json:"key"`
	Count int    `json:"count"`
}

// KeyValue stores name value pair
type KeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// NameMappedCounter stores column stats
type NameMappedCounter struct {
	Name        string       `json:"name"`
	MapCounters []MapCounter `json:"mapped"`
}

// MapCounter stores mapped counter
type MapCounter struct {
	Name  string `json:"name"`
	Count int    `json:"count"`
}
