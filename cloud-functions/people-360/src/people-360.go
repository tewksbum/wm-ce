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

var reAlphaNumeric = regexp.MustCompile("[^a-zA-Z0-9]+")

var redisTransientExpiration = 3600 * 24
var redisTemporaryExpiration = 3600

var ps *pubsub.Client
var topic *pubsub.Topic
var topic2 *pubsub.Topic
var status *pubsub.Topic
var cleanup *pubsub.Topic
var ds *datastore.Client
var fs *datastore.Client
var msp *redis.Pool

// var setSchema bigquery.Schema

func init() {
	ctx := context.Background()
	ps, _ = pubsub.NewClient(ctx, ProjectID)
	ds, _ = datastore.NewClient(ctx, ProjectID)
	fs, _ = datastore.NewClient(ctx, DSProjectID)
	topic = ps.Topic(os.Getenv("PSOUTPUT"))
	topic2 = ps.Topic(os.Getenv("PSOUTPUT2"))
	status = ps.Topic(os.Getenv("PSSTATUS"))
	cleanup = ps.Topic(os.Getenv("PSCLEANUP"))
	// delay the clean up by 1 min
	cleanup.PublishSettings.DelayThreshold = 60 * time.Second
	msp = &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial:        func() (redis.Conn, error) { return redis.Dial("tcp", os.Getenv("MEMSTORE")) },
	}
	log.Printf("init completed, pubsub topic name: %v", topic)
}

func People360(ctx context.Context, m PubSubMessage) error {
	var inputs []PeopleInput
	if err := json.Unmarshal(m.Data, &inputs); err != nil {
		log.Fatalf("Unable to unmarshal message %v with error %v", string(m.Data), err)
	}

	inputIsFromPost := false
	if value, ok := m.Attributes["source"]; ok {
		if value == "post" { // append signature only if the pubsub comes from post, do not append if it comes from cleanup
			inputIsFromPost = true
		}
	}

	var expiredSetCollection []string
	var dsNameSpace string

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
		if input.Signature.FiberType == "default" {
			existingCheck = GetRedisIntValue([]string{input.Signature.EventID, input.Signature.RecordID, "fiber"})
			if existingCheck == 1 { // this fiber has already been processed
				LogDev(fmt.Sprintf("Duplicate fiber detected %v", input.Signature))
				return nil
			}
		} else if input.Signature.FiberType == "mar" {
			existingCheck = GetRedisIntValue([]string{input.Signature.EventID, input.Signature.RecordID, "fiber"})
			if existingCheck == 0 { // default fiber has not been processed
				IncrRedisValue([]string{input.Signature.EventID, input.Signature.RecordID, "fiber-mar-retry"})
				retryCount := GetRedisIntValue([]string{input.Signature.EventID, input.Signature.RecordID, "fiber-mar-retry"})
				if retryCount < 30 {
					return fmt.Errorf("Default fiber not yet processed, retryn count  %v < max of 30, wait for retry", retryCount)
				}
			}
		}

		// store the fiber
		OutputPassthrough := ConvertPassthrough(input.Passthrough)
		var fiber PeopleFiber
		fiber.CreatedAt = time.Now()
		fiber.ID = uuid.New().String()
		fiber.MatchKeys = input.MatchKeys
		fiber.Passthrough = OutputPassthrough
		fiber.Signature = input.Signature

		// fiber in DS
		dsFiber := GetFiberDS(&fiber)
		dsNameSpace = strings.ToLower(fmt.Sprintf("%v-%v", Env, input.Signature.OwnerID))
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
			}
		} else if input.Signature.FiberType != "dupe" {
			// MAR and MPR are matchable always
			matchable = true
		}

		HasNewValues := false
		var output People360Output
		var FiberSignatures []Signature
		var FiberSearchFields []string
		output.ID = uuid.New().String()
		MatchKeyList := structs.Names(&PeopleOutput{})
		FiberMatchKeys := make(map[string][]string)
		// collect all fiber match key values
		for _, name := range MatchKeyList {
			FiberMatchKeys[name] = []string{}
		}
		var matchedFibers []string
		matchedDefaultFiber := 0

		if matchable {
			// locate existing set
			if len(input.Signature.RecordID) == 0 {
				// ensure record id is not blank or we'll have problem
				input.Signature.RecordID = uuid.New().String()
			}
			var searchFields []string
			searchFields = append(searchFields, fmt.Sprintf("RECORDID=%v", input.Signature.RecordID))
			if len(input.MatchKeys.EMAIL.Value) > 0 {
				searchFields = append(searchFields, fmt.Sprintf("EMAIL=%v", strings.TrimSpace(strings.ToUpper(input.MatchKeys.EMAIL.Value))))
			}
			if len(input.MatchKeys.PHONE.Value) > 0 && len(input.MatchKeys.FINITIAL.Value) > 0 {
				searchFields = append(searchFields, fmt.Sprintf("PHONE=%v&FINITIAL=%v", strings.TrimSpace(strings.ToUpper(input.MatchKeys.PHONE.Value)), strings.TrimSpace(strings.ToUpper(input.MatchKeys.FINITIAL.Value))))
			}
			if len(input.MatchKeys.CITY.Value) > 0 && len(input.MatchKeys.STATE.Value) > 0 && len(input.MatchKeys.LNAME.Value) > 0 && len(input.MatchKeys.FNAME.Value) > 0 && len(input.MatchKeys.AD1.Value) > 0 {
				searchFields = append(searchFields, fmt.Sprintf("FNAME=%v&LNAME=%v&AD1=%v&CITY=%v&STATE=%v", strings.TrimSpace(strings.ToUpper(input.MatchKeys.FNAME.Value)), strings.TrimSpace(strings.ToUpper(input.MatchKeys.LNAME.Value)), strings.TrimSpace(strings.ToUpper(input.MatchKeys.AD1.Value)), strings.TrimSpace(strings.ToUpper(input.MatchKeys.CITY.Value)), strings.TrimSpace(strings.ToUpper(input.MatchKeys.STATE.Value))))
			}
			LogDev(fmt.Sprintf("Search Fields: %+v", searchFields))
			keypattern := "*"
			redisKeys := GetRedisKeys(keypattern)
			redisValues := GetRedisValues(redisKeys)
			LogDev(fmt.Sprintf("redis matching keys: %+v, values %+v", redisKeys, redisValues))

			// read the FiberIDs from Redis
			searchKeys := [][]string{}
			searchSets := [][]string{}
			if len(searchFields) > 0 {
				for _, search := range searchFields {
					msKey := []string{input.Signature.OwnerID, "search", search}
					msSet := []string{input.Signature.OwnerID, "set", search}
					searchKeys = append(searchKeys, msKey)
					searchSets = append(searchSets, msSet)
				}
			}
			if len(searchKeys) > 0 {
				for _, searchKey := range searchKeys {
					searchValues := GetRedisValues(searchKey)
					if len(searchValues) > 0 {
						for _, searchValue := range searchValues {
							foundFibers := strings.Split(searchValue, ",")
							for _, foundFiber := range foundFibers {
								if len(foundFiber) > 20 { // make sure it is an actual id
									matchedFibers = append(matchedFibers, foundFiber)
								}
							}
						}
					}
				}
			}

			if len(searchSets) > 0 {
				for _, searchSet := range searchSets {
					searchValues := GetRedisValues(searchSet)
					if len(searchValues) > 0 {
						for _, searchValue := range searchValues {
							foundSets := strings.Split(searchValue, ",")
							for _, foundSet := range foundSets {
								if len(foundSet) > 20 { // make sure it is an actual id
									expiredSetCollection = append(expiredSetCollection, foundSet)
								}
							}
						}
					}
				}
			}
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
			if len(FiberKeys) > 0 {
				if err := fs.GetMulti(ctx, FiberKeys, Fibers); err != nil && err != datastore.ErrNoSuchEntity {
					log.Fatalf("Error fetching fibers ns %v kind %v, keys %v: %v,", dsNameSpace, DSKindFiber, FiberKeys, err)
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
		dsFiber.Search = GetPeopleFiberSearchFields(&dsFiber)

		var searchFields []string
		if dsFiber.Disposition != "dupe" && dsFiber.Disposition != "purge" {
			// get this into redis
			searchFields = append(searchFields, fmt.Sprintf("RECORDID=%v", input.Signature.RecordID))
			if len(dsFiber.EMAIL.Value) > 0 {
				searchFields = append(searchFields, fmt.Sprintf("EMAIL=%v", strings.TrimSpace(strings.ToUpper(dsFiber.EMAIL.Value))))
			}
			if len(dsFiber.PHONE.Value) > 0 && len(dsFiber.FINITIAL.Value) > 0 {
				searchFields = append(searchFields, fmt.Sprintf("PHONE=%v&FINITIAL=%v", strings.TrimSpace(strings.ToUpper(dsFiber.PHONE.Value)), strings.TrimSpace(strings.ToUpper(dsFiber.FINITIAL.Value))))
			}
			if len(dsFiber.CITY.Value) > 0 && len(dsFiber.STATE.Value) > 0 && len(dsFiber.LNAME.Value) > 0 && len(dsFiber.FNAME.Value) > 0 && len(dsFiber.AD1.Value) > 0 {
				searchFields = append(searchFields, fmt.Sprintf("FNAME=%v&LNAME=%v&AD1=%v&CITY=%v&STATE=%v", strings.TrimSpace(strings.ToUpper(dsFiber.FNAME.Value)), strings.TrimSpace(strings.ToUpper(dsFiber.LNAME.Value)), strings.TrimSpace(strings.ToUpper(dsFiber.AD1.Value)), strings.TrimSpace(strings.ToUpper(dsFiber.CITY.Value)), strings.TrimSpace(strings.ToUpper(dsFiber.STATE.Value))))
			}
			if len(searchFields) > 0 {
				for _, search := range searchFields {
					msKey := []string{input.Signature.OwnerID, "search", search}
					fiberKeys := GetRedisStringsValue(msKey)
					if !Contains(fiberKeys, dsFiber.ID.Name) {
						fiberKeys = append(fiberKeys, dsFiber.ID.Name)
						SetRedisTempKeyWithValue(msKey, strings.Join(fiberKeys, ","))
					}
				}
			}
		}
		// store the fiber
		if _, err := fs.Put(ctx, dsKey, &dsFiber); err != nil {
			log.Fatalf("Error: storing Fiber sig %v, error %v", input.Signature, err)
		}

		// stop processing if no new values
		// if !HasNewValues {
		// 	return nil
		// }
		if !matchable {
			LogDev(fmt.Sprintf("Unmatchable fiber detected %v", input.Signature))
			IncrRedisValue([]string{input.Signature.EventID, "fibers-deleted"})
			return nil
		}

		// append to the output value
		if inputIsFromPost { // append signature only if the pubsub comes from post, do not append if it comes from cleanup
			output.Signatures = append(FiberSignatures, input.Signature)
		}

		output.Signature = Signature360{
			OwnerID:   input.Signature.OwnerID,
			Source:    input.Signature.Source,
			EventID:   input.Signature.EventID,
			EventType: input.Signature.EventType,
		}
		if output.CreatedAt.IsZero() {
			output.CreatedAt = time.Now()
		}
		output.Fibers = append(matchedFibers, fiber.ID)
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
		log.Printf("golden search: %+v", goldenDS.Search)
		if _, err := fs.Put(ctx, goldenKey, &goldenDS); err != nil {
			log.Printf("Error: storing golden record with sig %v, error %v", input.Signature, err)
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

		if len(SetSearchFields) > 0 {
			for _, search := range SetSearchFields {
				msSet := []string{input.Signature.OwnerID, "set", search}
				setKeys := GetRedisStringsValue(msSet)
				if !Contains(setKeys, setDS.ID.Name) {
					setKeys = append(setKeys, setDS.ID.Name)
					SetRedisTempKeyWithValue(msSet, strings.Join(setKeys, ","))
				}
			}
		}

		setDS.Search = SetSearchFields
		log.Printf("set search: %+v", setDS.Search)
		if _, err := fs.Put(ctx, setKey, &setDS); err != nil {
			log.Printf("Error: storing set with sig %v, error %v", input.Signature, err)
		}

		if input.Signature.FiberType == "default" {
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
			LogDev(fmt.Sprintf("record finished ? %v; fiber finished ? %v", recordFinished, fiberFinished))
			if recordFinished && fiberFinished {
				eventData := EventData{
					Signature: input.Signature,
					EventData: make(map[string]interface{}),
				}
				eventData.EventData["status"] = "Finished"
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
		}

		LogDev(fmt.Sprintf("deleting expired sets %v and expired golden records %v", SetKeys, GoldenKeys))
		if err := fs.DeleteMulti(ctx, SetKeys); err != nil {
			log.Printf("Error: deleting expired sets: %v", err)
		}
		if err := fs.DeleteMulti(ctx, GoldenKeys); err != nil {
			log.Printf("Error: deleting expired golden records: %v", err)
		}
	}

	return nil
}
