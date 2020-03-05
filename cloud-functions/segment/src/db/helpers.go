package db

import (
	"segment/models"
)

func buildPeopleDecode(r *models.PeopleRecord, signature string /*, setPeopleID bool*/) models.Record {
	rs := &models.DecodeRecord{}
	rs.BaseRecord = models.BaseRecord{
		OwnerID:     r.GetOwnerID(),
		EntityType:  models.TypeDecode,
		Source:      r.GetSource(),
		Owner:       r.GetOwner(),
		Passthrough: r.GetPassthrough(),
		Attributes:  r.GetAttributes(),
		Timestamp:   r.GetTimestamp(),
	}
	rs.IDField = models.DecodeIDField
	rs.ColumnList = models.DecodeColumnList
	rs.ColumnBlackList = models.DecodeBlackList
	rs.SelectColumnList = models.DefaultSelectColumnList
	rs.DBopts = models.Options{
		Type:               models.CSQL,
		TablenamePrefix:    models.TblnamePrefix,
		Tablename:          models.TblDecode,
		TablenameSuffix:    rs.GetStrOwnerID(),
		SchemaName:         r.GetDBOptions().SchemaName,
		IsPartitioned:      false,
		HasTablenameSuffix: true,
		HasTablenamePrefix: true,
	}
	// if setPeopleID {
	rs.PeopleID = r.Record.PeopleID
	// }
	rs.Signature = signature
	return rs
}

func buildHouseholdDecode(r *models.HouseholdRecord, signature string) models.Record {
	rs := &models.DecodeRecord{}
	rs.BaseRecord = models.BaseRecord{
		OwnerID:     r.GetOwnerID(),
		EntityType:  models.TypeDecode,
		Source:      r.GetSource(),
		Owner:       r.GetOwner(),
		Passthrough: r.GetPassthrough(),
		Attributes:  r.GetAttributes(),
		Timestamp:   r.GetTimestamp(),
	}
	rs.IDField = models.DecodeIDField
	rs.ColumnList = models.DecodeColumnList
	rs.ColumnBlackList = models.DecodeBlackList
	rs.SelectColumnList = models.DefaultSelectColumnList
	rs.DBopts = models.Options{
		Type:               models.CSQL,
		TablenamePrefix:    models.TblnamePrefix,
		Tablename:          models.TblDecode,
		TablenameSuffix:    rs.GetStrOwnerID(),
		SchemaName:         r.GetDBOptions().SchemaName,
		IsPartitioned:      false,
		HasTablenameSuffix: true,
		HasTablenamePrefix: true,
	}
	rs.HouseholdID = r.Record.HouseholdID
	rs.Signature = signature
	return rs
}

func buildExpiredSet(r models.Record, expiredID string, entity string) models.Record {
	rs := &models.ExpiredSetRecord{}
	rs.BaseRecord = models.BaseRecord{
		OwnerID:    r.GetOwnerID(),
		EntityType: models.TypeExpiredSet,
		Owner:      r.GetOwner(),
	}
	rs.IDField = models.ExpiredSetIDField
	rs.ColumnList = models.ExpiredSetColumnList
	rs.ColumnBlackList = models.ExpiredSetBlackList
	rs.SelectColumnList = models.ExpiredSetColumnList
	rs.DBopts = models.Options{
		Type:               models.CSQL,
		TablenamePrefix:    models.TblnamePrefix,
		Tablename:          models.TblExpiredSet,
		TablenameSuffix:    rs.GetStrOwnerID(),
		SchemaName:         r.GetDBOptions().SchemaName,
		IsPartitioned:      false,
		HasTablenameSuffix: true,
		HasTablenamePrefix: true,
	}
	rs.ExpiredID = expiredID
	rs.Entity = entity
	return rs
}
