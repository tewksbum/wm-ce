package house720

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/datastore"
	"cloud.google.com/go/pubsub"
	"github.com/fatih/structs"
	"github.com/gomodule/redigo/redis"
	"github.com/google/uuid"
)

var ProjectID = os.Getenv("PROJECTID")
var DSProjectID = os.Getenv("DSPROJECTID")
var Env = os.Getenv("ENVIRONMENT")
var dev = Env == "dev"
var DSKindSet = os.Getenv("DSKINDSET")
var DSKindGolden = os.Getenv("DSKINDGOLDEN")
var DSKindFiber = os.Getenv("DSKINDFIBER")

var cfName = os.Getenv("FUNCTION_NAME")

var ctx context.Context
var ds *datastore.Client
var fs *datastore.Client
var ps *pubsub.Client
var msp *redis.Pool
var topic *pubsub.Topic
var topicR *pubsub.Topic

func init() {
	ctx = context.Background()
	ds, _ = datastore.NewClient(ctx, ProjectID)
	fs, _ = datastore.NewClient(ctx, DSProjectID)
	msp = &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 10 * time.Second,
		Dial:        func() (redis.Conn, error) { return redis.Dial("tcp", os.Getenv("MEMSTORE")) },
	}
	ps, _ = pubsub.NewClient(ctx, ProjectID)
	topic = ps.Topic(os.Getenv("PSOUTPUT"))
	topicR = ps.Topic(os.Getenv("PSREPORT"))
	// ready.PublishSettings.DelayThreshold = 120 * time.Second
}

func House720(ctx context.Context, m PubSubMessage) error {

	var input FileComplete
	if err := json.Unmarshal(m.Data, &input); err != nil {
		log.Fatalf("Unable to unmarshal message %v with error %v", string(m.Data), err)
	}
	log.Printf("Checking sets for event %v", string(m.Data))

	cleanupKey := []string{input.EventID, "house-cleanup"}
	if GetRedisIntValue(cleanupKey) == 1 { // already processed
		return nil
	}
	SetRedisTempKey(cleanupKey)

	report0 := FileReport{
		ID:          input.EventID,
		StatusLabel: "set verification started",
		StatusBy:    cfName,
		StatusTime:  time.Now(),
	}
	publishReport(&report0, cfName)

	ownerNS := strings.ToLower(fmt.Sprintf("%v-%v", Env, input.OwnerID))

	// we'll fetch the fibers associated with the event, and then run search key against sets, if we get more than 1 hit, we'll send this fiber back to 360
	var eventFibers []HouseFiberDS // this is for raw fibers
	fiberQuery := datastore.NewQuery(DSKindFiber).Namespace(ownerNS).Filter("eventid =", input.EventID)
	if _, err := fs.GetAll(ctx, fiberQuery, &eventFibers); err != nil {
		log.Fatalf("Error querying fibers: %v", err)
		return nil
	}
	if eventFibers == nil {
		return nil
	}
	publishReport(&FileReport{
		ID: input.EventID,
		Counters: []ReportCounter{
			ReportCounter{
				Type:      "House720",
				Name:      "fibers:before",
				Count:     len(eventFibers),
				Increment: false,
			},
		},
	}, cfName)

	var eventFiberSearchKeys []HouseFiberDSProjected
	for _, f := range eventFibers {
		eventFiberSearchKeys = append(eventFiberSearchKeys, HouseFiberDSProjected{
			ID:          f.ID,
			Search:      f.Search,
			Disposition: f.Disposition,
			FiberType:   f.FiberType,
		})
	}
	eventFibers = nil // clear eventFibers to release memory

	var eventSets []HouseSetDS // this is for raw sets
	if _, err := fs.GetAll(ctx, datastore.NewQuery(DSKindSet).Namespace(ownerNS).Filter("eventid =", input.EventID), &eventSets); err != nil {
		log.Fatalf("Error querying sets: %v", err)
		return nil
	}
	if eventSets == nil {
		return nil
	}

	publishReport(&FileReport{
		ID: input.EventID,
		Counters: []ReportCounter{
			ReportCounter{
				Type:      "House720",
				Name:      "sets:before",
				Count:     len(eventSets),
				Increment: false,
			},
			ReportCounter{
				Type:      "House720",
				Name:      "sets:after",
				Count:     len(eventSets),
				Increment: true,
			},
		},
	}, cfName)

	var eventSetSearchKeys []HouseSetDSProjected
	var es []string
	for _, f := range eventSets {
		// es := make([]string)
		for _, fs := range f.Search {
			if strings.HasPrefix(fs, "HOUSE") {
				es = append(es, fs)
			}
		}
		log.Printf("addings searchKeys: %v", es)
		eventSetSearchKeys = append(eventSetSearchKeys, HouseSetDSProjected{
			ID:     f.ID,
			Search: es,
		})
		es = nil
	}
	eventSets = nil // clear eventFibers to release memory

	// var eventSets []HouseSetDS // this is for raw sets

	// type HouseSetDS struct {
	// 	ID                     *datastore.Key `datastore:"__key__"`
	// 	OwnerID                []string       `datastore:"ownerid"`
	// 	Source                 []string       `datastore:"source"`
	// 	EventID                []string       `datastore:"eventid"`
	// 	EventType              []string       `datastore:"eventtype"`
	// 	FiberType              []string       `datastore:"fibertype"`
	// 	RecordID               []string       `datastore:"recordid"`
	// 	RecordIDNormalized     []string       `datastore:"recordidnormalized"`
	// 	CreatedAt              time.Time      `datastore:"createdat"`
	// 	Fibers                 []string       `datastore:"fibers"`
	// 	Search                 []string       `datastore:"search"`

	// type HouseSetDSProjected struct {
	// 	ID     *datastore.Key `datastore:"__key__"`
	// 	Search []string       `datastore:"search"`
	// }

	// for _, f := range eventSets {
	// 	eventSetSearchKeys = append(eventSetSearchKeys, PeopleSetDSProjected{
	// 		ID:     f.ID,
	// 		Search: f.Search,
	// 	})
	// }

	// reorganize sets as a map
	setSearchMap := make(map[string][]string)
	for _, s := range eventSetSearchKeys { // each set
		for _, ss := range s.Search { // each search key of each set
			if len(ss) > 0 { // in case we have a blank
				if setIDs, ok := setSearchMap[ss]; ok {
					if !Contains(setIDs, s.ID.Name) {
						setSearchMap[ss] = append(setIDs, s.ID.Name)
					}
				} else {
					setSearchMap[ss] = []string{s.ID.Name}
				}
			}
		}
	}

	for { // keep running this until we no longer have sets sharing the same search key
		log.Println("running a loop")
		// loop through fiber list and find where search key appears in more than 1 set search key
		var reprocessFibers []string
		var missingFibers []string
		for _, f := range eventFiberSearchKeys { // each fiber
			for _, s := range f.Search { // each search key of each fiber
				if strings.HasPrefix(s, "HOUSE") {
					if setIDs, ok := setSearchMap[s]; ok { // in the search key map
						if len(setIDs) > 1 {
	
							reprocessFibers = append(reprocessFibers, f.ID.Name)
	
							// load the existing sets
							var reportCounters []ReportCounter
							var existingSetKeys []*datastore.Key
							var existingSets []HouseSetDS
							for _, setID := range setIDs {
								dsSetGetKey := datastore.NameKey(DSKindSet, setID, nil)
								dsSetGetKey.Namespace = ownerNS
								existingSetKeys = append(existingSetKeys, dsSetGetKey)
								existingSets = append(existingSets, HouseSetDS{})
							}
							if len(existingSetKeys) > 0 {
								if err := fs.GetMulti(ctx, existingSetKeys, existingSets); err != nil && err != datastore.ErrNoSuchEntity {
									log.Printf("ERROR fetching sets ns %v kind %v, keys %v: %v,", ownerNS, DSKindSet, existingSetKeys, err)
								}
							}
							setsJSON, _ := json.Marshal(existingSets)
							log.Printf("search key %v found multi sets %v: %v", s, setIDs, string(setsJSON))
	
							var allFiberIDs []string
							var allFiberKeys []*datastore.Key
							var allFibers []HouseFiberDS
	
							newSetSignatures := []Signature{}
							for _, es := range existingSets {
								for _, ef := range es.Fibers {
									if !Contains(allFiberIDs, ef) {
										allFiberIDs = append(allFiberIDs, ef)
										dsFiberGetKey := datastore.NameKey(DSKindFiber, ef, nil)
										dsFiberGetKey.Namespace = ownerNS
										allFiberKeys = append(allFiberKeys, dsFiberGetKey)
										allFibers = append(allFibers, HouseFiberDS{})
									}
								}
							}
							if len(allFiberKeys) > 0 {
								if err := fs.GetMulti(ctx, allFiberKeys, allFibers); err != nil && err != datastore.ErrNoSuchEntity {
									log.Printf("ERROR fetching fibers ns %v kind %v, keys %v: %v,", ownerNS, DSKindFiber, allFiberKeys, err)
								}
							}
	
							var MatchKeysFromFiber []MatchKey360
							MatchKeyList := structs.Names(&HouseOutput{})
							FiberMatchKeys := make(map[string][]string)
							// collect all fiber match key values
							for _, name := range MatchKeyList {
								FiberMatchKeys[name] = []string{}
							}
	
							// build signatures and matchkeys for the new set
							for _, ef := range allFibers {
								fiberSignature := Signature{
									OwnerID:   ef.OwnerID,
									Source:    ef.Source,
									EventID:   ef.EventID,
									EventType: ef.EventType,
									RecordID:  ef.RecordID,
									FiberID:   ef.ID.Name,
								}
								if !ContainsSignature(newSetSignatures, fiberSignature) {
									newSetSignatures = append(newSetSignatures, fiberSignature)
								}
								for _, name := range MatchKeyList {
									value := strings.TrimSpace(GetMatchKeyFieldFromDSFiber(&ef, name).Value)
									if len(value) > 0 && !Contains(FiberMatchKeys[name], value) {
										FiberMatchKeys[name] = append(FiberMatchKeys[name], value)
									}
								}
							}
	
							// check to see if there are any new values
							for _, name := range MatchKeyList {
								mk360 := MatchKey360{
									Key:    name,
									Values: FiberMatchKeys[name],
								}
								MatchKeysFromFiber = append(MatchKeysFromFiber, mk360)
							}
	
							newSetID := uuid.New().String()
							var setDS HouseSetDS
							setKey := datastore.NameKey(DSKindSet, newSetID, nil)
							setKey.Namespace = ownerNS
							setDS.ID = setKey
							setDS.Fibers = allFiberIDs
							setDS.CreatedAt = time.Now()
							PopulateSetOutputSignatures(&setDS, newSetSignatures)
							PopulateSetOutputMatchKeys(&setDS, MatchKeysFromFiber)
	
							var goldenDS HouseGoldenDS
							goldenKey := datastore.NameKey(DSKindGolden, newSetID, nil)
							goldenKey.Namespace = ownerNS
							goldenDS.ID = goldenKey
							goldenDS.CreatedAt = time.Now()
							PopulateGoldenOutputMatchKeys(&goldenDS, MatchKeysFromFiber)
							goldenDS.Search = GetHouseGoldenSearchFields(&goldenDS)
							if _, err := fs.Put(ctx, goldenKey, &goldenDS); err != nil {
								log.Printf("Error: storing golden record error %v", err)
							}
	
							reportCounters = append(reportCounters, ReportCounter{
								Type:      "House720",
								Name:      "multisets",
								Count:     1,
								Increment: true,
							})
	
							reportCounters = append(reportCounters,
								ReportCounter{
									Type:      "House:Golden",
									Name:      "Unique",
									Count:     1,
									Increment: true,
								},
							)
	
							SetRedisKeyWithExpiration([]string{input.EventID, newSetID, "house-golden"})
							if goldenDS.ADVALID == "TRUE" {
								SetRedisKeyWithExpiration([]string{input.EventID, newSetID, "house-golden", "advalid"})
								reportCounters = append(reportCounters,
									ReportCounter{
										Type:      "House:Golden",
										Name:      "IsAdValid",
										Count:     1,
										Increment: true,
									},
								)
							}
							if len(goldenDS.EMAIL) > 0 {
								SetRedisKeyWithExpiration([]string{input.EventID, newSetID, "house-golden", "email"})
								reportCounters = append(reportCounters,
									ReportCounter{
										Type:      "House:Golden",
										Name:      "HasEmail",
										Count:     1,
										Increment: true,
									},
								)
							}
	
							// populate search fields for set from a) existing sets b) new fiber c) golden
							var newSetSearchFields []string
							for _, ef := range allFibers {
								for _, search := range ef.Search {
									if !Contains(newSetSearchFields, search) {
										newSetSearchFields = append(newSetSearchFields, search)
									}
								}
							}
							if len(goldenDS.Search) > 0 {
								for _, search := range goldenDS.Search {
									if !Contains(newSetSearchFields, search) {
										newSetSearchFields = append(newSetSearchFields, search)
									}
								}
							}
	
							setDS.Search = newSetSearchFields
							if _, err := fs.Put(ctx, setKey, &setDS); err != nil {
								log.Printf("Error: storing set error %v", err)
							}
	
							// put the set search key in redis -- is this still necessary?  we are already in 720
							if len(newSetSearchFields) > 0 {
								for _, search := range newSetSearchFields {
									msSet := []string{input.OwnerID, "house-search-sets", search}
									AppendRedisTempKey(msSet, setDS.ID.Name)
								}
							}
							// write each of the search key into each of the fiber in redis
							for _, search := range newSetSearchFields {
								msKey := []string{input.OwnerID, "house-search-fibers", search}
								for _, f := range setDS.Fibers {
									AppendRedisTempKey(msKey, f)
								}
							}
	
							reportCounters = append(reportCounters,
								ReportCounter{
									Type:      "House720",
									Name:      "sets:created",
									Count:     1,
									Increment: true,
								},
								ReportCounter{
									Type:      "House720",
									Name:      "sets:after",
									Count:     1,
									Increment: true,
								},
							)
	
							setList := []SetDetail{ // the new set
								SetDetail{
									ID:         newSetID,
									CreatedOn:  time.Now(),
									FiberCount: len(setDS.Fibers),
								},
							}
							// expire the existing sets and goldens
							var expiringSetKeys []*datastore.Key
							var expiringGoldenKeys []*datastore.Key
	
							for _, set := range setIDs {
								setKey := datastore.NameKey(DSKindSet, set, nil)
								setKey.Namespace = ownerNS
								expiringSetKeys = append(expiringSetKeys, setKey)
								goldenKey := datastore.NameKey(DSKindGolden, set, nil)
								goldenKey.Namespace = ownerNS
								expiringGoldenKeys = append(expiringGoldenKeys, goldenKey)
	
								setList = append(setList, SetDetail{ // the expired set
									ID:         set,
									IsDeleted:  true,
									DeletedOn:  time.Now(),
									ReplacedBy: newSetID,
								})
	
								// we'll decrement some counters here
								if SetRedisKeyIfNotExists([]string{set, "house-golden", "deleted"}) == 1 { // able to set the value, first time we are deleting
									// let's see what we are deleting
									if GetRedisIntValue([]string{input.EventID, set, "house-golden"}) == 1 { // this is a golden from the event that just got deleted
										reportCounters = append(reportCounters,
											ReportCounter{
												Type:      "House:Golden",
												Name:      "Unique",
												Count:     -1,
												Increment: true,
											},
										)
										if GetRedisIntValue([]string{input.EventID, set, "house-golden", "advalid"}) == 1 {
											reportCounters = append(reportCounters,
												ReportCounter{
													Type:      "House:Golden",
													Name:      "IsAdValid",
													Count:     -1,
													Increment: true,
												},
											)
										}
	
										if GetRedisIntValue([]string{input.EventID, set, "house-golden", "email"}) == 1 {
											reportCounters = append(reportCounters,
												ReportCounter{
													Type:      "House:Golden",
													Name:      "HasEmail",
													Count:     -1,
													Increment: true,
												},
											)
										}
									}
								}
							}
	
							LogDev(fmt.Sprintf("deleting expired sets %v and expired golden records %v", expiringSetKeys, expiringGoldenKeys))
							if err := fs.DeleteMulti(ctx, expiringSetKeys); err != nil {
								log.Printf("Error: deleting expired sets: %v", err)
							}
							if err := fs.DeleteMulti(ctx, expiringGoldenKeys); err != nil {
								log.Printf("Error: deleting expired golden records: %v", err)
							}
							reportCounters = append(reportCounters,
								ReportCounter{
									Type:      "House720",
									Name:      "sets:expired",
									Count:     len(setIDs),
									Increment: true,
								},
								ReportCounter{
									Type:      "House720",
									Name:      "sets:after",
									Count:     -len(setIDs),
									Increment: true,
								},
							)
	
							// remove expired set id from searchKeyMap and add new one
							for _, s := range newSetSearchFields {
								updatedSetList := []string{newSetID}
								for _, l := range setSearchMap[s] {
									if !Contains(setIDs, l) {
										updatedSetList = append(updatedSetList, l)
									}
								}
								setSearchMap[s] = updatedSetList
							}
	
							// publish report
							publishReport(&FileReport{
								ID:       input.EventID,
								Counters: reportCounters,
								SetList:  setList,
							}, cfName)
	
							log.Printf("Merged sets %v into a set %v", setIDs, newSetID)
							break // go on to next fiber
						}
					} else {
						if f.Disposition != "purge" && f.Disposition != "dupe" {
							missingFibers = append(missingFibers, f.ID.Name) // reprocess these too
						}
						log.Printf("WARN fiber id %v type %v disposition %v search key %v not in a set", f.ID.Name, f.FiberType, f.Disposition, fs)
					}					
				}
				
			}
		}
		if len(reprocessFibers) == 0 { // we are done
			break
		} else {
			publishReport(&FileReport{
				ID: input.EventID,
				Counters: []ReportCounter{
					ReportCounter{
						Type:      "House720",
						Name:      "Reprocess",
						Count:     len(reprocessFibers),
						Increment: true,
					},
				},
			}, cfName)

		}

	}
	return nil
}
