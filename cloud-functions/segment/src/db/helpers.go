package db

import (
	"segment/models"
)

func buildDecode(r *models.PeopleRecord, signature string) models.Record {
	rs := &models.DecodeRecord{}
	rs.BaseRecord = models.BaseRecord{
		OwnerID:     r.GetOwnerID(),
		EntityType:  r.GetEntityType(),
		Source:      r.GetSource(),
		Owner:       r.GetOwner(),
		Passthrough: r.GetPassthrough(),
		Attributes:  r.GetAttributes(),
		Timestamp:   r.GetTimestamp(),
	}
	rs.IDField = models.DecodeIDField
	rs.ColumnList = models.DecodeColumnList
	rs.ColumnBlackList = models.DecodeBlackList
	rs.DBopts = models.Options{
		Type:               models.CSQL,
		Tablename:          rs.GetStrOwnerID(),
		TablenamePrefix:    models.TblnamePrefix,
		TablenameSuffix:    models.TblDecode,
		IsPartitioned:      false,
		HasTablenameSuffix: true,
		HasTablenamePrefix: true,
	}
	rs.PeopleID = r.Record.PeopleID
	rs.Signature = signature
	return rs
}
