package models

import (
	"segment/utils"
	"segment/utils/logger"
	"strings"
)

func opToSQL(op string, fields ...string) string {
	switch c := len(fields); {
	case c > 1:
		for i := 0; i < len(fields); i++ {
			if fields[i] == "" {
				fields[i] = "?"
			}
		}
	case c == 1:
		fields = []string{fields[0], "?"}
	default:
		fields = []string{"?", "?"}
	}
	return map[string]string{
		OperationEquals:           " = " + fields[0],
		OperationNotEquals:        " != " + fields[0],
		OperationLessThan:         " < " + fields[0],
		OperationLessThanEqual:    " <= " + fields[0],
		OperationGreaterThan:      " > " + fields[0],
		OperationGreaterThanEqual: " >= " + fields[0],
		OperationIs:               " IS " + fields[0],
		OperationIsNull:           " IS NULL",
		OperationIsNotNull:        " IS NOT NULL",
		OperationIn:               " IN (" + fields[0] + ")",
		OperationNotIn:            " NOT IN (" + fields[0] + ")",
		OperationLike:             " LIKE " + fields[0],
		OperationILike:            " ILIKE " + fields[0],
		OperationNotLike:          " NOT LIKE " + fields[0],
		OperationBetween:          " BETWEEN " + fields[0] + " AND " + fields[1],
	}[op]
}

// ParseFilters parses the filter and adds the where condition to the transaction
func ParseFilters(filters []QueryFilter, useFieldName bool, prefix string, removeThisNesting string) ([]ParsedQueryFilter, error) {
	var pq []ParsedQueryFilter
	for i, f := range filters {
		if f.OpType != "" && f.OpType != OperationTypeFilter {
			logger.ErrFmtStr("This query filter is not an OperationTypeFilter")
			continue
		}
		if f.Op == "" {
			return pq, logger.ErrFmtStr("Filter needs an operation to work")
		}
		if i > 0 && f.OpLink == nil {
			return pq, logger.ErrFmtStr("Need an link operator to concatenate more than one filter")
		}
		field := strings.ReplaceAll(f.Field, removeThisNesting+".", "") + utils.Itoa(i+1)
		op := ""
		if useFieldName {
			op = opToSQL(f.Op, prefix+field)
		} else {
			opToSQL(f.Op, "")
		}
		pf := ParsedQueryFilter{
			ParamNames:      []string{field},
			ParsedCondition: utils.ToSnakeCase(f.Field) + op,
		}
		if f.Value != nil {
			pf.Values = []interface{}{f.Value}
		}
		if f.Values != nil {
			pf.Values = []interface{}{f.Values}
		}
		if i > 0 && f.OpLink != nil && (*f.OpLink == OperationLinkOr || *f.OpLink == OperationLinkAnd) {
			pf.ParsedCondition = *f.OpLink + " " + pf.ParsedCondition
		}
		pf.ParsedCondition = " " + pf.ParsedCondition
		switch f.Op {
		case OperationBetween:
			if len(f.Values) != 2 {
				return pq, logger.ErrFmtStr("Operation [%#v] needs an array with exactly two items in [values] field", f.Op)
			}
			pf.ParamNames = []string{field + "1", field + "2"}
		case OperationIn, OperationNotIn:
			if len(pf.Values) < 1 {
				return pq, logger.ErrFmtStr("Operation [%#v] needs an array with at least 1 item on [values] field", f.Op)
			}
		case OperationIsNotNull:
			fallthrough
		case OperationIsNull:
			if f.Value != nil {
				return pq, logger.ErrFmtStr("Operation [%#v] needs the field [value] to be null", f.Op)
			}
		default:
			if f.Value == nil {
				return pq, logger.ErrFmtStr("Operation [%#v] needs the field [value] to compare", f.Op)
			}
		}
		pq = append(pq, pf)
	}
	return pq, nil
}

// ParseOrderBy parses the filter and adds the where condition to the transaction
func ParseOrderBy(filters []QueryFilter) string {
	var by string
	for _, f := range filters {
		if f.OpType != OperationTypeOrderBy {
			// logger.ErrFmtStr("This query type is not is not an OperationTypeOrderBy %#v", f)
			continue
		}
		if strings.Contains(by, f.Field) {
			continue
		}
		op := strings.ToUpper(f.Op)
		if op != strings.ToUpper(OperationOrderByAsc) &&
			op != strings.ToUpper(OperationOrderByDesc) {
			op = OperationOrderByAsc
		}
		by += by + f.Field + " " + op + ","
	}
	if len(by) > 0 {
		oby := " ORDER BY " + by
		if oby[len(oby)-1] == ',' {
			oby = oby[:len(oby)-1]
		}
		return oby
	}
	return ""
}

// GetRecordType gets the proper interface by entity type
func GetRecordType(entityType string) interface{} {
	switch entityType {
	case TypeCampaign:
		return &Campaign{}
	case TypeEvent:
		return &Event{}
	case TypeHousehold:
		return &Household{}
	case TypeOrderHeader:
		return &OrderHeader{}
	case TypeOrderConsignment:
		return &OrderConsignment{}
	case TypeOrderDetail:
		return &OrderDetail{}
	case TypePeople:
		return &People{}
	case TypeProduct:
		return &Product{}
	case TypeDecode:
		return &DecodeRecord{}
	default:
		return &FallbackData{}
	}
}
