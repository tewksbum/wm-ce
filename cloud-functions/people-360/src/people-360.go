package people360

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"regexp"
	"sort"
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
var SetTableName = os.Getenv("SETTABLE")
var FiberTableName = os.Getenv("FIBERTABLE")
var ESUrl = os.Getenv("ELASTICURL")
var ESUid = os.Getenv("ELASTICUSER")
var ESPwd = os.Getenv("ELASTICPWD")
var ESIndex = os.Getenv("ELASTICINDEX")
var Env = os.Getenv("ENVIRONMENT")
var dev = Env == "dev"
var DSKindSet = os.Getenv("DSKINDSET")
var DSKindGolden = os.Getenv("DSKINDGOLDEN")
var DSKindFiber = os.Getenv("DSKINDFIBER")

var cfName = os.Getenv("FUNCTION_NAME")

var reAlphaNumeric = regexp.MustCompile("[^a-zA-Z0-9]+")

var redisTransientExpiration = 3600 * 24
var redisTemporaryExpiration = 3600
var reGraduationYear = regexp.MustCompile(`^20\d{2}$`)
var reGraduationYear2 = regexp.MustCompile(`^\d{2}$`)
var reClassYearFY1 = regexp.MustCompile(`^FY\d{4}$`)

var ctx context.Context
var ps *pubsub.Client
var topic *pubsub.Topic
var topic2 *pubsub.Topic
var status *pubsub.Topic
var cleanup *pubsub.Topic
var ds *datastore.Client
var fs *datastore.Client
var msp *redis.Pool
var cpTopic *pubsub.Topic

var topicR *pubsub.Topic

func init() {
	ctx = context.Background()
	ps, _ = pubsub.NewClient(ctx, ProjectID)
	ds, _ = datastore.NewClient(ctx, ProjectID)
	fs, _ = datastore.NewClient(ctx, DSProjectID)
	topic = ps.Topic(os.Getenv("PSOUTPUT"))
	topic2 = ps.Topic(os.Getenv("PSOUTPUT2"))
	status = ps.Topic(os.Getenv("PSSTATUS"))
	cleanup = ps.Topic(os.Getenv("PSCLEANUP"))
	cpTopic = ps.Topic(os.Getenv("PSCPFILE"))
	// delay the clean up by 1 min
	cleanup.PublishSettings.DelayThreshold = 60 * time.Second
	msp = &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial:        func() (redis.Conn, error) { return redis.Dial("tcp", os.Getenv("MEMSTORE")) },
	}
	topicR = ps.Topic(os.Getenv("PSREPORT"))
	log.Printf("init completed, pubsub topic name: %v", topic)
}

func People360(ctx context.Context, m PubSubMessage) error {
	var inputs []PeopleInput
	if err := json.Unmarshal(m.Data, &inputs); err != nil {
		log.Printf("Unable to unmarshal message %v with error %v", string(m.Data), err)
	}
	LogDev(fmt.Sprintf("input is:\n%v", string(m.Data)))
	inputIsFromPost := false
	defaultIsPurged := false
	if value, ok := m.Attributes["source"]; ok {
		if value == "post" || value == "test" { // append signature only if the pubsub comes from post, do not append if it comes from cleanup
			inputIsFromPost = true
		}
	}
	LogDev(fmt.Sprintf("input is from post: %v", inputIsFromPost))

	for _, input := range inputs {
		// assign first initial and zip5
		if len(input.MatchKeys.FNAME.Value) > 0 {
			input.MatchKeys.FINITIAL = MatchKeyField{
				Value:  input.MatchKeys.FNAME.Value[0:1],
				Source: input.MatchKeys.FNAME.Source,
			}
		}
		if len(input.MatchKeys.ZIP.Value) > 5 {
			input.MatchKeys.ZIP5 = MatchKeyField{
				Value:  input.MatchKeys.ZIP.Value[0:5],
				Source: input.MatchKeys.ZIP.Source,
			}
		}
		existingCheck := 0

		// only perform the duplicate detection if it is coming from post, do not do it otherwise, such as from cleanup
		if inputIsFromPost {
			if input.Signature.FiberType == "default" {
				existingCheck = GetRedisIntValue([]string{input.Signature.EventID, input.Signature.RecordID, "fiber"})
				if existingCheck == 1 { // this fiber has already been processed
					LogDev(fmt.Sprintf("Duplicate fiber detected %v", input.Signature))
					report := FileReport{
						ID: input.Signature.EventID,
						Counters: []ReportCounter{
							ReportCounter{
								Type:      "People360:Audit",
								Name:      "DupeMessage",
								Count:     1,
								Increment: true,
							},
						},
					}
					publishReport(&report, cfName)
					return nil
				}
			}
			// else if input.Signature.FiberType == "mar" && inputIsFromPost { // if it came from 720, skip the default check for mar
			// 	existingCheck = GetRedisIntValue([]string{input.Signature.EventID, input.Signature.RecordID, "fiber"})
			// 	if existingCheck == 0 { // default fiber has not been processed
			// 		IncrRedisValue([]string{input.Signature.EventID, input.Signature.RecordID, "fiber-mar-retry"})
			// 		retryCount := GetRedisIntValue([]string{input.Signature.EventID, input.Signature.RecordID, "fiber-mar-retry"})
			// 		report := FileReport{
			// 			ID: input.Signature.EventID,
			// 			Counters: []ReportCounter{
			// 				ReportCounter{
			// 					Type:      "People360:Audit",
			// 					Name:      "Retry",
			// 					Count:     1,
			// 					Increment: true,
			// 				},
			// 			},
			// 		}
			// 		publishReport(&report, cfName)
			// 		if retryCount < 30 {
			// 			report := FileReport{
			// 				ID: input.Signature.EventID,
			// 				Counters: []ReportCounter{
			// 					ReportCounter{
			// 						Type:      "People360:Audit",
			// 						Name:      "RetryExceeded",
			// 						Count:     1,
			// 						Increment: true,
			// 					},
			// 				},
			// 			}
			// 			publishReport(&report, cfName)
			// 			return fmt.Errorf("Default fiber not yet processed, retryn count  %v < max of 30, wait for retry", retryCount)
			// 		}
			// 	}
			// }
		}

		// store the fiber
		OutputPassthrough := ConvertPassthrough(input.Passthrough)
		var fiber PeopleFiber
		fiber.CreatedAt = time.Now()
		fiber.ID = input.Signature.FiberID
		fiber.MatchKeys = input.MatchKeys
		fiber.Passthrough = OutputPassthrough
		fiber.Signature = input.Signature

		log.Printf("fiber id is %v", fiber.ID)

		// fiber in DS
		dsFiber := GetFiberDS(&fiber)
		dsNameSpace := strings.ToLower(fmt.Sprintf("%v-%v", Env, input.Signature.OwnerID))
		dsKey := datastore.NameKey(DSKindFiber, fiber.ID, nil)
		dsKey.Namespace = dsNameSpace
		dsFiber.ID = dsKey

		matchable := false
		if input.Signature.FiberType == "default" {
			if len(input.MatchKeys.EMAIL.Value) > 0 ||
				(len(input.MatchKeys.PHONE.Value) > 0 && len(input.MatchKeys.FINITIAL.Value) > 0) ||
				(len(input.MatchKeys.CITY.Value) > 0 &&
					(len(input.MatchKeys.STATE.Value) > 0 || (len(input.MatchKeys.COUNTRY.Value) > 0 && input.MatchKeys.COUNTRY.Value != "US")) &&
					len(input.MatchKeys.LNAME.Value) > 0 &&
					len(input.MatchKeys.FNAME.Value) > 0 &&
					len(input.MatchKeys.AD1.Value) > 0) {
				matchable = true
			} else {
				defaultIsPurged = true
			}
		} else if input.Signature.FiberType != "dupe" {
			// MAR and MPR are matchable always
			matchable = true
			// except when default is already purged then mar needs to be purged too
			if input.Signature.FiberType == "mar" && defaultIsPurged {
				matchable = false
			}
		}

		HasNewValues := false
		var output People360Output
		var FiberSignatures []Signature
		var FiberSearchFields []string
		output.ID = uuid.New().String()
		output.Signatures = []Signature{}
		MatchKeyList := structs.Names(&PeopleOutput{})
		FiberMatchKeys := make(map[string][]string)
		// collect all fiber match key values
		for _, name := range MatchKeyList {
			FiberMatchKeys[name] = []string{}
		}
		var matchedFibers []string
		matchedDefaultFiber := 0
		var expiredSetCollection []string
		reportCounters1 := []ReportCounter{}
		reportCounters2 := []ReportCounter{}
		if matchable {
			// locate existing set
			if len(input.Signature.RecordID) == 0 {
				// ensure record id is not blank or we'll have problem
				input.Signature.RecordID = uuid.New().String()
			}
			var searchFields []string
			searchFields = append(searchFields, fmt.Sprintf("RECORDID=%v", input.Signature.RecordID))
			if len(input.MatchKeys.EMAIL.Value) > 0 {
				searchFields = append(searchFields, fmt.Sprintf("EMAIL=%v&ROLE=%v", strings.TrimSpace(strings.ToUpper(input.MatchKeys.EMAIL.Value)), strings.TrimSpace(strings.ToUpper(input.MatchKeys.ROLE.Value))))
			}
			if len(input.MatchKeys.PHONE.Value) > 0 && len(input.MatchKeys.FINITIAL.Value) > 0 {
				searchFields = append(searchFields, fmt.Sprintf("PHONE=%v&FINITIAL=%v&ROLE=%v", strings.TrimSpace(strings.ToUpper(input.MatchKeys.PHONE.Value)), strings.TrimSpace(strings.ToUpper(input.MatchKeys.FINITIAL.Value)), strings.TrimSpace(strings.ToUpper(input.MatchKeys.ROLE.Value))))
			}
			if len(input.MatchKeys.CITY.Value) > 0 && len(input.MatchKeys.STATE.Value) > 0 && len(input.MatchKeys.LNAME.Value) > 0 && len(input.MatchKeys.FNAME.Value) > 0 && len(input.MatchKeys.AD1.Value) > 0 {
				searchFields = append(searchFields, fmt.Sprintf("FNAME=%v&LNAME=%v&AD1=%v&CITY=%v&STATE=%v&ROLE=%v", strings.TrimSpace(strings.ToUpper(input.MatchKeys.FNAME.Value)), strings.TrimSpace(strings.ToUpper(input.MatchKeys.LNAME.Value)), strings.TrimSpace(strings.ToUpper(input.MatchKeys.AD1.Value)), strings.TrimSpace(strings.ToUpper(input.MatchKeys.CITY.Value)), strings.TrimSpace(strings.ToUpper(input.MatchKeys.STATE.Value)), strings.TrimSpace(strings.ToUpper(input.MatchKeys.ROLE.Value))))
			}
			LogDev(fmt.Sprintf("Search Fields: %+v", searchFields))

			// read the FiberIDs from Redis
			searchKeys := []string{}
			searchSets := []string{}
			if len(searchFields) > 0 {
				for _, search := range searchFields {
					msKey := []string{input.Signature.OwnerID, "search-fibers", search}
					msSet := []string{input.Signature.OwnerID, "search-sets", search}
					searchKeys = append(searchKeys, strings.Join(msKey, ":"))
					searchSets = append(searchSets, strings.Join(msSet, ":"))
				}
			}

			if len(searchKeys) > 0 {
				searchValues := GetRedisGuidValuesList(searchKeys)
				if len(searchValues) > 0 {
					for _, searchValue := range searchValues {
						for _, searchVal := range searchValue {
							if len(searchVal) > 0 {
								if !Contains(matchedFibers, searchVal) {
									matchedFibers = append(matchedFibers, searchVal)
								}
							}
						}
					}
				}
			}
			setCardinality := "noset"
			if len(searchSets) > 0 {
				searchValues := GetRedisGuidValuesList(searchSets)
				if len(searchValues) > 0 {
					for _, searchValue := range searchValues {
						for _, searchVal := range searchValue {
							if len(searchVal) > 0 {
								if !Contains(expiredSetCollection, searchVal) {
									expiredSetCollection = append(expiredSetCollection, searchVal)
								}
							}
						}
					}
				}
			}
			switch len(expiredSetCollection) {
			case 1:
				setCardinality = "oneset"
				break
			case 0:
				setCardinality = "noset"
				break
			default:
				setCardinality = "multisets"
			}

			reportCounters1 = append(reportCounters1, ReportCounter{
				Type:      "People360",
				Name:      setCardinality,
				Count:     1,
				Increment: true,
			})
			LogDev(fmt.Sprintf("Matched Fibers: %+v", matchedFibers))

			// get all the Fibers
			var FiberKeys []*datastore.Key
			var Fibers []PeopleFiberDS
			for _, fiber := range matchedFibers {
				dsFiberGetKey := datastore.NameKey(DSKindFiber, fiber, nil)
				dsFiberGetKey.Namespace = dsNameSpace
				FiberKeys = append(FiberKeys, dsFiberGetKey)
				Fibers = append(Fibers, PeopleFiberDS{})
			}
			// loop 10 times if we can't load the fiber
			if len(FiberKeys) > 0 {
				retryLoop := 0
				for {
					if err := fs.GetMulti(ctx, FiberKeys, Fibers); err != nil && err != datastore.ErrNoSuchEntity {
						log.Printf("Error fetching fibers ns %v kind %v, keys %v: %v,", dsNameSpace, DSKindFiber, FiberKeys, err)
						if strings.HasSuffix(err.Error(), "no such entity") {
							if retryLoop > 10 {
								break
							}
							time.Sleep(1 * time.Second)
							retryLoop++
						} else {
							break
						}

					} else {
						break
					}
				}
			}

			// sort by createdAt desc
			sort.Slice(Fibers, func(i, j int) bool {
				return Fibers[i].CreatedAt.After(Fibers[j].CreatedAt)
			})

			LogDev(fmt.Sprintf("Fibers: %v", Fibers))

			for i, fiber := range Fibers {
				LogDev(fmt.Sprintf("loaded fiber %v of %v: %v", i, len(Fibers), fiber))
				FiberSignatures = append(FiberSignatures, Signature{
					OwnerID:   fiber.OwnerID,
					Source:    fiber.Source,
					EventType: fiber.EventType,
					EventID:   fiber.EventID,
					FiberType: fiber.FiberType,
					RecordID:  fiber.RecordID,
				})

				if len(fiber.RecordID) == 0 {
					log.Printf("WARN fier is missing signature fields %v", fiber)
				}

				if fiber.FiberType == "default" {
					matchedDefaultFiber++
				}

				if len(fiber.Search) > 0 {
					for _, s := range fiber.Search {
						FiberSearchFields = append(FiberSearchFields, s)
					}
				}

				for _, name := range MatchKeyList {
					value := strings.TrimSpace(GetMatchKeyFieldFromDSFiber(&fiber, name).Value)
					if len(value) > 0 && !Contains(FiberMatchKeys[name], value) {
						FiberMatchKeys[name] = append(FiberMatchKeys[name], value)
					}
				}
				LogDev(fmt.Sprintf("FiberMatchKey values %v", FiberMatchKeys))
			}
			var MatchKeysFromFiber []MatchKey360

			// check to see if there are any new values
			for _, name := range MatchKeyList {
				mk360 := MatchKey360{
					Key:    name,
					Values: FiberMatchKeys[name],
				}

				newValue := strings.TrimSpace(GetMatchKeyFieldFromStruct(&input.MatchKeys, name).Value)
				if len(newValue) > 0 {
					if !Contains(mk360.Values, newValue) {
						LogDev(fmt.Sprintf("new values found %v, %v for key %v, chars are %v", mk360.Values, newValue, name, ToAsciiArray(newValue)))
						HasNewValues = true
					}
				}

				MatchKeysFromFiber = append(MatchKeysFromFiber, mk360)
				// LogDev(fmt.Sprintf("mk.Values %v: %v", name, FiberMatchKeys[name]))
			}

			output.MatchKeys = MatchKeysFromFiber

		} else {
			if inputIsFromPost {
				reportCounters1 = append(reportCounters1,
					ReportCounter{
						Type:      "People360",
						Name:      "Unmatchable",
						Count:     1,
						Increment: true,
					},
					ReportCounter{
						Type:      "People360:Audit",
						Name:      "Unmatchable:" + input.Signature.FiberType,
						Count:     1,
						Increment: true,
					},
				)
			}
		}

		log.Printf("FiberSearchFields is %+v", FiberSearchFields)

		if !matchable {
			if input.Signature.FiberType == "dupe" {
				dsFiber.Disposition = "dupe"
			} else {
				dsFiber.Disposition = "purge"
			}
		} else if matchedDefaultFiber == 0 {
			dsFiber.Disposition = "new"
		} else if !HasNewValues {
			dsFiber.Disposition = "dupe"
		} else {
			dsFiber.Disposition = "update"
		}
		if inputIsFromPost {
			reportCounters1 = append(reportCounters1,
				ReportCounter{
					Type:      "People360",
					Name:      "Total",
					Count:     1,
					Increment: true,
				},
				ReportCounter{
					Type:      "People360:Audit",
					Name:      "Disposition:" + dsFiber.Disposition,
					Count:     1,
					Increment: true,
				},
				ReportCounter{
					Type:      "People360:Audit",
					Name:      "Disposition:" + fiber.Signature.FiberType + ":" + dsFiber.Disposition,
					Count:     1,
					Increment: true,
				},
			)

			if dsFiber.Disposition == "dupe" { // these dupes covers both dupes identified in post as well as no new values identified in 360
				reportCounters1 = append(reportCounters1,
					ReportCounter{
						Type:      "People360",
						Name:      "Dupe",
						Count:     1,
						Increment: true,
					}, ReportCounter{
						Type:      "People360:Audit",
						Name:      "Dupe:" + fiber.Signature.FiberType,
						Count:     1,
						Increment: true,
					},
				)
			} else if dsFiber.Disposition == "purge" {
				reportCounters1 = append(reportCounters1,
					ReportCounter{
						Type:      "People360:Audit",
						Name:      "Purge",
						Count:     1,
						Increment: true,
					}, ReportCounter{
						Type:      "People360:Audit",
						Name:      "Purge:" + fiber.Signature.FiberType,
						Count:     1,
						Increment: true,
					},
				)
			} else if dsFiber.Disposition == "new" {
				reportCounters1 = append(reportCounters1,
					ReportCounter{
						Type:      "People360:Audit",
						Name:      "New",
						Count:     1,
						Increment: true,
					}, ReportCounter{
						Type:      "People360:Audit",
						Name:      "New:" + fiber.Signature.FiberType,
						Count:     1,
						Increment: true,
					},
				)
			} else if dsFiber.Disposition == "update" {
				reportCounters1 = append(reportCounters1,
					ReportCounter{
						Type:      "People360:Audit",
						Name:      "Update",
						Count:     1,
						Increment: true,
					}, ReportCounter{
						Type:      "People360:Audit",
						Name:      "Update:" + fiber.Signature.FiberType,
						Count:     1,
						Increment: true,
					},
				)
			}
		}

		fiberList := []FiberDetail{
			FiberDetail{
				ID:          input.Signature.FiberID,
				Disposition: dsFiber.Disposition,
			},
		}

		dsFiber.Search = GetPeopleFiberSearchFields(&dsFiber)

		// if dsFiber.Disposition != "dupe" && dsFiber.Disposition != "purge" {
		if dsFiber.Disposition != "purge" {
			for _, search := range dsFiber.Search {
				msKey := []string{input.Signature.OwnerID, "search-fibers", search}
				AppendRedisTempKey(msKey, dsFiber.ID.Name)
			}
		}

		// store the fiber
		if _, err := fs.Put(ctx, dsKey, &dsFiber); err != nil {
			log.Fatalf("Error: storing Fiber sig %v, error %v", input.Signature, err)
		}

		if !matchable {
			LogDev(fmt.Sprintf("Unmatchable fiber detected %v", input.Signature))
			IncrRedisValue([]string{input.Signature.EventID, "fibers-deleted"})

			report := FileReport{
				ID:        input.Signature.EventID,
				Counters:  reportCounters1,
				FiberList: fiberList,
			}
			publishReport(&report, cfName)
			continue
		}

		reportCounters1 = append(reportCounters1,
			ReportCounter{
				Type:      "People360:Audit",
				Name:      "Golden:Created",
				Count:     1,
				Increment: true,
			},
			ReportCounter{
				Type:      "Golden",
				Name:      "Unique",
				Count:     1,
				Increment: true,
			},
		)

		if fiber.Signature.FiberType == "mpr" {
			reportCounters1 = append(reportCounters1,
				ReportCounter{
					Type:      "Golden:MPR",
					Name:      "Unique",
					Count:     1,
					Increment: true,
				},
				ReportCounter{
					Type:      "People360:Audit",
					Name:      "Golden:Created:MPR",
					Count:     1,
					Increment: true,
				},
			)
		} else {
			reportCounters1 = append(reportCounters1,
				ReportCounter{
					Type:      "Golden:NonMPR",
					Name:      "Unique",
					Count:     1,
					Increment: true,
				},
				ReportCounter{
					Type:      "People360:Audit",
					Name:      "Golden:Created:NonMPR",
					Count:     1,
					Increment: true,
				},
			)
		}

		reportCounters1 = append(reportCounters1,
			ReportCounter{
				Type:      "People360:Audit",
				Name:      "Set:Created",
				Count:     1,
				Increment: true,
			},
		)

		output.Signatures = append(FiberSignatures, input.Signature)
		output.Signature = Signature360{
			OwnerID:   input.Signature.OwnerID,
			Source:    input.Signature.Source,
			EventID:   input.Signature.EventID,
			EventType: input.Signature.EventType,
		}
		if output.CreatedAt.IsZero() {
			output.CreatedAt = time.Now()
		}
		if !Contains(matchedFibers, fiber.ID) {
			matchedFibers = append(matchedFibers, fiber.ID)
		}
		output.Fibers = matchedFibers
		output.Passthroughs = OutputPassthrough
		//output.TrustedIDs = append(output.TrustedIDs, input.MatchKeys.CAMPAIGNID.Value)
		var OutputMatchKeys []MatchKey360
		for _, name := range MatchKeyList {
			mk := GetMatchKey360ByName(output.MatchKeys, name)
			mk.Key = name
			mk.Value = strings.TrimSpace(GetMatchKeyFieldFromStruct(&input.MatchKeys, name).Value)
			// if blank, assign it a value
			if len(mk.Value) == 0 && len(mk.Values) > 0 {
				mk.Value = mk.Values[0]
			}
			if len(mk.Value) > 0 && !Contains(mk.Values, mk.Value) {
				mk.Values = append(mk.Values, mk.Value)
			}

			// special rules for assigning values
			if name == "TITLE" {
				mk.Value = GetSmallestYear(mk.Values)
			}

			if name == "ADVALID" {
				mk.Value = GetAdValid(mk.Values)
			}

			OutputMatchKeys = append(OutputMatchKeys, *mk)
		}
		output.MatchKeys = OutputMatchKeys

		// record the set id in DS
		var setDS PeopleSetDS
		setKey := datastore.NameKey(DSKindSet, output.ID, nil)
		setKey.Namespace = dsNameSpace
		setDS.ID = setKey
		setDS.Fibers = output.Fibers
		setDS.CreatedAt = output.CreatedAt
		PopulateSetOutputSignatures(&setDS, output.Signatures)
		PopulateSetOutputMatchKeys(&setDS, output.MatchKeys)

		var goldenDS PeopleGoldenDS
		goldenKey := datastore.NameKey(DSKindGolden, output.ID, nil)
		goldenKey.Namespace = dsNameSpace
		goldenDS.ID = goldenKey
		goldenDS.CreatedAt = output.CreatedAt
		PopulateGoldenOutputMatchKeys(&goldenDS, output.MatchKeys)
		goldenDS.Search = GetPeopleGoldenSearchFields(&goldenDS)
		goldenJSON, _ := json.Marshal(goldenDS)
		log.Printf("writing golden %v", string(goldenJSON))
		if _, err := fs.Put(ctx, goldenKey, &goldenDS); err != nil {
			log.Printf("Error: storing golden record with sig %v, error %v", input.Signature, err)
		}

		// track golden status in redis for future expire lookup
		SetRedisKeyWithExpiration([]string{input.Signature.EventID, output.ID, "golden"})
		if goldenDS.ADVALID == "TRUE" {
			SetRedisKeyWithExpiration([]string{input.Signature.EventID, output.ID, "golden", "advalid"})
			reportCounters1 = append(reportCounters1,
				ReportCounter{
					Type:      "People360:Audit",
					Name:      "Golden:Created:IsAdValid",
					Count:     1,
					Increment: true,
				},
				ReportCounter{
					Type:      "Golden",
					Name:      "IsAdValid",
					Count:     1,
					Increment: true,
				},
			)
			if fiber.Signature.FiberType == "mpr" {
				reportCounters1 = append(reportCounters1,
					ReportCounter{
						Type:      "Golden:MPR",
						Name:      "IsAdValid",
						Count:     1,
						Increment: true,
					},
				)
			} else {
				reportCounters1 = append(reportCounters1,
					ReportCounter{
						Type:      "Golden:NonMPR",
						Name:      "IsAdValid",
						Count:     1,
						Increment: true,
					},
				)
				reportCounters2 = append(reportCounters2,
					ReportCounter{
						Type:      "SchoolYear:" + input.Passthrough["schoolYear"],
						Name:      "Mailable:" + validateStatus(goldenDS.STATUS),
						Count:     1,
						Increment: true,
					},
				)
				if goldenDS.COUNTRY != "US" {
					SetRedisKeyWithExpiration([]string{input.Signature.EventID, output.ID, "golden", "international"})
					reportCounters2 = append(reportCounters2,
						ReportCounter{
							Type:      "SchoolYear:" + input.Passthrough["schoolYear"],
							Name:      "International:" + validateStatus(goldenDS.STATUS),
							Count:     1,
							Increment: true,
						},
					)
				}
			}
		} else {
			SetRedisKeyWithExpiration([]string{input.Signature.EventID, output.ID, "golden", "noadvalid"})
			if fiber.Signature.FiberType != "mpr" {
				reportCounters2 = append(reportCounters2,
					ReportCounter{
						Type:      "SchoolYear:" + input.Passthrough["schoolYear"],
						Name:      "NoMailable:" + validateStatus(goldenDS.STATUS),
						Count:     1,
						Increment: true,
					},
				)
			}
		}
		if len(goldenDS.EMAIL) > 0 {
			SetRedisKeyWithExpiration([]string{input.Signature.EventID, output.ID, "golden", "email"})
			reportCounters1 = append(reportCounters1,
				ReportCounter{
					Type:      "People360:Audit",
					Name:      "Golden:Created:HasEmail",
					Count:     1,
					Increment: true,
				},
				ReportCounter{
					Type:      "Golden",
					Name:      "HasEmail",
					Count:     1,
					Increment: true,
				},
			)

			if fiber.Signature.FiberType == "mpr" {
				reportCounters1 = append(reportCounters1,
					ReportCounter{
						Type:      "Golden:MPR",
						Name:      "HasEmail",
						Count:     1,
						Increment: true,
					},
				)
			} else {
				reportCounters1 = append(reportCounters1,
					ReportCounter{
						Type:      "Golden:NonMPR",
						Name:      "HasEmail",
						Count:     1,
						Increment: true,
					},
				)
				reportCounters2 = append(reportCounters2,
					ReportCounter{
						Type:      "SchoolYear:" + input.Passthrough["schoolYear"],
						Name:      "HasEmail:" + validateStatus(goldenDS.STATUS),
						Count:     1,
						Increment: true,
					},
				)
			}
		}

		var SetSearchFields []string
		// populate search fields for set from a) existing sets b) new fiber c) golden
		if len(FiberSearchFields) > 0 {
			for _, search := range FiberSearchFields {
				if !Contains(SetSearchFields, search) {
					SetSearchFields = append(SetSearchFields, search)
				}
			}
		}

		if len(dsFiber.Search) > 0 {
			for _, search := range dsFiber.Search {
				if !Contains(SetSearchFields, search) {
					SetSearchFields = append(SetSearchFields, search)
				}
			}
		}

		if len(goldenDS.Search) > 0 {
			for _, search := range goldenDS.Search {
				if !Contains(SetSearchFields, search) {
					SetSearchFields = append(SetSearchFields, search)
				}
			}
		}

		// put the set search key in redis
		if len(SetSearchFields) > 0 {
			for _, search := range SetSearchFields {
				msSet := []string{input.Signature.OwnerID, "search-sets", search}
				AppendRedisTempKey(msSet, setDS.ID.Name)
			}
		}

		setDS.Search = SetSearchFields

		// write each of the search key into each of the fiber in redis
		for _, search := range SetSearchFields {
			msKey := []string{input.Signature.OwnerID, "search-fibers", search}
			for _, f := range setDS.Fibers {
				AppendRedisTempKey(msKey, f)
			}
		}

		setList := []SetDetail{
			SetDetail{
				ID:         output.ID,
				CreatedOn:  time.Now(),
				FiberCount: len(setDS.Fibers),
			},
		}
		fiberList = append(fiberList,
			FiberDetail{
				ID: fiber.ID,
				Sets: []string{
					output.ID,
				},
			},
		)

		log.Printf("set search: %+v", setDS.Search)
		if len(setDS.EventID) == 0 {
			setJSON, _ := json.Marshal(setDS)
			log.Printf("ERROR*** set has blank event id: %v", string(setJSON))
		}

		if _, err := fs.Put(ctx, setKey, &setDS); err != nil {
			log.Printf("Error: storing set with sig %v, error %v", input.Signature, err)
		}

		if len(expiredSetCollection) > 0 {
			// remove expired sets and setmembers from DS
			var SetKeys []*datastore.Key
			// var MemberKeys []*datastore.Key
			var GoldenKeys []*datastore.Key

			for _, set := range expiredSetCollection {
				setKey := datastore.NameKey(DSKindSet, set, nil)
				setKey.Namespace = dsNameSpace
				SetKeys = append(SetKeys, setKey)
				goldenKey := datastore.NameKey(DSKindGolden, set, nil)
				goldenKey.Namespace = dsNameSpace
				GoldenKeys = append(GoldenKeys, goldenKey)
				setList = append(setList, SetDetail{
					ID:         set,
					IsDeleted:  true,
					DeletedOn:  time.Now(),
					ReplacedBy: output.ID,
				})

				// we'll decrement some counters here
				if SetRedisKeyIfNotExists([]string{set, "golden", "deleted"}) == 1 { // able to set the value, first time we are deleting
					// let's see what we are deleting
					if GetRedisIntValue([]string{input.Signature.EventID, set, "golden"}) == 1 { // this is a golden from the event that just got deleted
						reportCounters1 = append(reportCounters1,
							ReportCounter{
								Type:      "Golden",
								Name:      "Unique",
								Count:     -1,
								Increment: true,
							},
						)
						if GetRedisIntValue([]string{input.Signature.EventID, set, "golden", "advalid"}) == 1 {
							reportCounters1 = append(reportCounters1,
								ReportCounter{
									Type:      "Golden",
									Name:      "IsAdValid",
									Count:     -1,
									Increment: true,
								},
							)
						}

						if GetRedisIntValue([]string{input.Signature.EventID, set, "golden", "email"}) == 1 {
							reportCounters1 = append(reportCounters1,
								ReportCounter{
									Type:      "Golden",
									Name:      "HasEmail",
									Count:     -1,
									Increment: true,
								},
							)
						}
					}
				}

				if fiber.Signature.FiberType == "mpr" {
					if SetRedisKeyIfNotExists([]string{set, "golden:mpr", "deleted"}) == 1 { // able to set the value, first time we are deleting
						// let's see what we are deleting
						if GetRedisIntValue([]string{input.Signature.EventID, set, "golden"}) == 1 { // this is a golden from the event that just got deleted
							reportCounters1 = append(reportCounters1,
								ReportCounter{
									Type:      "Golden:MPR",
									Name:      "Unique",
									Count:     -1,
									Increment: true,
								},
							)
							if GetRedisIntValue([]string{input.Signature.EventID, set, "golden", "advalid"}) == 1 {
								reportCounters1 = append(reportCounters1,
									ReportCounter{
										Type:      "Golden:MPR",
										Name:      "IsAdValid",
										Count:     -1,
										Increment: true,
									},
								)
							}

							if GetRedisIntValue([]string{input.Signature.EventID, set, "golden", "email"}) == 1 {
								reportCounters1 = append(reportCounters1,
									ReportCounter{
										Type:      "Golden:MPR",
										Name:      "HasEmail",
										Count:     -1,
										Increment: true,
									},
								)
							}
						}
					}
				} else {
					if SetRedisKeyIfNotExists([]string{set, "golden:nonmpr", "deleted"}) == 1 { // able to set the value, first time we are deleting
						// let's see what we are deleting
						if GetRedisIntValue([]string{input.Signature.EventID, set, "golden"}) == 1 { // this is a golden from the event that just got deleted
							reportCounters1 = append(reportCounters1,
								ReportCounter{
									Type:      "Golden:NonMPR",
									Name:      "Unique",
									Count:     -1,
									Increment: true,
								},
							)
							if GetRedisIntValue([]string{input.Signature.EventID, set, "golden", "advalid"}) == 1 {
								reportCounters1 = append(reportCounters1,
									ReportCounter{
										Type:      "Golden:NonMPR",
										Name:      "IsAdValid",
										Count:     -1,
										Increment: true,
									},
								)
								reportCounters2 = append(reportCounters2,
									ReportCounter{
										Type:      "SchoolYear:" + input.Passthrough["schoolYear"],
										Name:      "Mailable:" + validateStatus(goldenDS.STATUS),
										Count:     -1,
										Increment: true,
									},
								)
								if GetRedisIntValue([]string{input.Signature.EventID, set, "golden", "international"}) == 1 {
									reportCounters2 = append(reportCounters2,
										ReportCounter{
											Type:      "SchoolYear:" + input.Passthrough["schoolYear"],
											Name:      "International:" + validateStatus(goldenDS.STATUS),
											Count:     -1,
											Increment: true,
										},
									)
								}
							}
							if GetRedisIntValue([]string{input.Signature.EventID, set, "golden", "noadvalid"}) == 1 {
								reportCounters2 = append(reportCounters2,
									ReportCounter{
										Type:      "SchoolYear:" + input.Passthrough["schoolYear"],
										Name:      "NoMailable:" + validateStatus(goldenDS.STATUS),
										Count:     -1,
										Increment: true,
									},
								)
							}
							if GetRedisIntValue([]string{input.Signature.EventID, set, "golden", "email"}) == 1 {
								reportCounters1 = append(reportCounters1,
									ReportCounter{
										Type:      "Golden:NonMPR",
										Name:      "HasEmail",
										Count:     -1,
										Increment: true,
									},
								)
								reportCounters2 = append(reportCounters2,
									ReportCounter{
										Type:      "SchoolYear:" + input.Passthrough["schoolYear"],
										Name:      "HasEmail:" + validateStatus(goldenDS.STATUS),
										Count:     -1,
										Increment: true,
									},
								)
							}
						}
					}
				}
			}

			// setCardinality := "noset"
			// if len(SetKeys) == 1 {
			// 	setCardinality = "oneset"
			// } else if len(SetKeys) > 1 {
			// 	setCardinality = "multisets"
			// }
			// reportCounters1 = append(reportCounters1, ReportCounter{
			// 	Type:      "People360",
			// 	Name:      setCardinality,
			// 	Count:     1,
			// 	Increment: true,
			// })

			LogDev(fmt.Sprintf("deleting expired sets %v and expired golden records %v", SetKeys, GoldenKeys))
			if err := fs.DeleteMulti(ctx, SetKeys); err != nil {
				log.Printf("Error: deleting expired sets: %v", err)
			}
			if err := fs.DeleteMulti(ctx, GoldenKeys); err != nil {
				log.Printf("Error: deleting expired golden records: %v", err)
			}

			reportCounters1 = append(reportCounters1, ReportCounter{Type: "People360:Audit", Name: "Set:Expired", Count: len(expiredSetCollection), Increment: true})
			reportCounters1 = append(reportCounters1, ReportCounter{Type: "People360:Audit", Name: "Golden:Expired", Count: len(expiredSetCollection), Increment: true})

		}

		if input.Signature.FiberType == "default" {
			reportCounters1 = append(reportCounters1, ReportCounter{Type: "People360:Audit", Name: "Fiber:Completed", Count: 1, Increment: true})
			IncrRedisValue([]string{input.Signature.EventID, "fibers-completed"})
			SetRedisKeyWithExpiration([]string{input.Signature.EventID, input.Signature.RecordID, "fiber"})

			// grab the count and see if we are done
			counters := GetRedisIntValues([][]string{
				[]string{input.Signature.EventID, "records-total"},
				[]string{input.Signature.EventID, "records-completed"},
				[]string{input.Signature.EventID, "records-deleted"},
				[]string{input.Signature.EventID, "fibers-completed"},
				[]string{input.Signature.EventID, "fibers-deleted"},
			})
			LogDev(fmt.Sprintf("Received response from redis %v", counters))
			recordCount, recordCompleted, recordDeleted, fiberCompleted, fiberDeleted := 0, 0, 0, 0, 0
			if len(counters) == 5 {
				recordCount = counters[0]
				recordCompleted = counters[1]
				recordDeleted = counters[2]
				fiberCompleted = counters[3]
				fiberDeleted = counters[4]
			}
			recordFinished := false
			fiberFinished := false
			if recordCompleted+recordDeleted >= recordCount && recordCount > 0 {
				recordFinished = true
			}
			if fiberCompleted+fiberDeleted >= recordCount && recordCount > 0 {
				fiberFinished = true
			}

			if recordCount > 0 {
				percentRecordFinished := fmt.Sprintf("%d%%", 10*int(10.0*float32(recordCompleted+recordDeleted)/float32(recordCount)))
				progressKey := []string{input.Signature.EventID, percentRecordFinished}
				if percentRecordFinished != "0%" { // do not write 0%
					if GetRedisIntValue(progressKey) == 1 { // already published this status
					} else {
						SetRedisTempKey(progressKey)
						report := FileReport{
							ID:          input.Signature.EventID,
							StatusLabel: "records progress " + percentRecordFinished,
							StatusBy:    cfName,
							StatusTime:  time.Now(),
						}
						if percentRecordFinished == "100%" {
							report.ProcessingEnd = time.Now()
						}
						publishReport(&report, cfName)
					}
				}
			}

			LogDev(fmt.Sprintf("record finished ? %v; fiber finished ? %v, rc = %v, rf = %v, rd = %v, fc = %v, fd =%v", recordFinished, fiberFinished, recordCount, recordCompleted, recordDeleted, fiberCompleted, fiberDeleted))
			if recordFinished && fiberFinished {
				eventData := EventData{
					Signature: input.Signature,
					EventData: make(map[string]interface{}),
				}
				eventData.EventData["status"] = "Records Setted"
				eventData.EventData["message"] = fmt.Sprintf("Processed %v records and %v fibers, purged %v records and %v fibers", recordCompleted, fiberCompleted, recordDeleted, fiberDeleted)
				eventData.EventData["records-total"] = recordCount
				eventData.EventData["records-completed"] = recordCompleted
				eventData.EventData["records-deleted"] = recordDeleted
				eventData.EventData["fibers-completed"] = fiberCompleted
				eventData.EventData["fibers-deleted"] = fiberDeleted
				statusJSON, _ := json.Marshal(eventData)
				psresult := status.Publish(ctx, &pubsub.Message{
					Data: statusJSON,
				})
				_, err := psresult.Get(ctx)
				if err != nil {
					log.Fatalf("%v Could not pub status to pubsub: %v", input.Signature.EventID, err)
				}

				cleanupKey := []string{input.Signature.EventID, "cleanup-sent"}
				if GetRedisIntValue(cleanupKey) == 1 { // already processed
					report := FileReport{
						ID:            input.Signature.EventID,
						ProcessingEnd: time.Now(),
						StatusLabel:   "records already done",
						StatusBy:      cfName,
						StatusTime:    time.Now(),
					}
					publishReport(&report, cfName)
				} else {
					SetRedisTempKey(cleanupKey)
					if inputIsFromPost { // only pub this message if the source is from post, do not pub if 720
						finished := FileComplete{
							EventID: input.Signature.EventID,
							OwnerID: input.Signature.OwnerID,
						}
						finishedJSON, _ := json.Marshal(finished)
						pcresult := cleanup.Publish(ctx, &pubsub.Message{
							Data: finishedJSON,
						})
						_, err = pcresult.Get(ctx)
						if err != nil {
							log.Fatalf("%v Could not pub cleanup to pubsub: %v", input.Signature.EventID, err)
						}
					}
				}
			}
		} else if input.Signature.FiberType == "mar" {
			SetRedisKeyWithExpiration([]string{input.Signature.EventID, input.Signature.RecordID, "fiber-mar"})
		}

		{
			report := FileReport{
				ID:        input.Signature.EventID,
				Counters:  reportCounters1,
				SetList:   setList,
				FiberList: fiberList,
			}
			publishReport(&report, cfName)
		}

		{
			report := FileReport{
				ID:        strings.ToLower(input.Signature.OwnerID),
				Counters:  reportCounters2,
				SetList:   setList,
				FiberList: fiberList,
			}
			publishReport(&report, cfName)
		}

		// push into pubsub
		output.ExpiredSets = expiredSetCollection
		outputJSON, _ := json.Marshal(output)
		psresult := topic.Publish(ctx, &pubsub.Message{
			Data: outputJSON,
			Attributes: map[string]string{
				"type":   "people",
				"source": "360",
			},
		})
		psid, err := psresult.Get(ctx)
		_, err = psresult.Get(ctx)
		if err != nil {
			log.Printf("Error: %v Could not pub to pubsub: %v", input.Signature.EventID, err)
		} else {
			LogDev(fmt.Sprintf("%v pubbed record as message id %v: %v", input.Signature.EventID, psid, string(outputJSON)))
		}

		topic2.Publish(ctx, &pubsub.Message{
			Data: outputJSON,
			Attributes: map[string]string{
				"type":   "people",
				"source": "360",
			},
		})

		//Send to cp-file for eventType == "Form Submission"
		if input.Signature.EventType == "Form Submission" {
			outputCP := FileReady{
				EventID: input.Signature.EventID,
				OwnerID: input.Signature.OwnerID,
			}
			outputCPJSON, _ := json.Marshal(outputCP)
			cpresult := cpTopic.Publish(ctx, &pubsub.Message{
				Data: outputCPJSON,
			})
			_, err = cpresult.Get(ctx)
			if err != nil {
				log.Fatalf("%v Could not pub cleanup to pubsub: %v", input.Signature.EventID, err)
			}
		}
	}

	return nil
}
