package preprocess

import (
	"context"
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"
	"google.golang.org/api/ml/v1"

	"github.com/gomodule/redigo/redis"
)

type PubSubMessage struct {
	Data []byte `json:"data"`
}

type Signature struct {
	OwnerID   int64  `json:"ownerId"`
	Source    string `json:"source"`
	EventID   string `json:"eventId"`
	EventType string `json:"eventType"`
	RecordID  string `json:"recordId"`
}

type Input struct {
	Signature   Signature         `json:"signature"`
	Passthrough map[string]string `json:"passthrough"`
	Fields      map[string]string `json:"fields"`
	Attributes  map[string]string `json:"attributes"`
}

type Prediction struct {
	Predictions []float64 `json:"predictions"`
}

type MLInput struct {
	Instances [][]float64 `json:"instances"`
}

type PeopleERR struct {
	Address1        int `json:"Address1"`
	Address2        int `json:"Address2"`
	Age             int `json:"Age"`
	Birthday        int `json:"Birthday"`
	City            int `json:"City"`
	Country         int `json:"Country"`
	County          int `json:"County"`
	Email           int `json:"Email"`
	FirstName       int `json:"FirstName"`
	FullName        int `json:"FullName"`
	Gender          int `json:"Gender"`
	LastName        int `json:"LastName"`
	MiddleName      int `json:"MiddleName"`
	ParentEmail     int `json:"ParentEmail"`
	ParentFirstName int `json:"ParentFirstName"`
	ParentLastName  int `json:"ParentLastName"`
	ParentName      int `json:"ParentName"`
	Phone           int `json:"Phone"`
	State           int `json:"State"`
	Suffix          int `json:"Suffix"`
	ZipCode         int `json:"ZipCode"`
	TrustedID       int `json:"TrustedID"`
	Title           int `json:"Title"`
	Role            int `json:"Role"`
}

type NER struct {
	FAC       float64 `json:"FAC"`
	GPE       float64 `json:"GPE"`
	LOC       float64 `json:"LOC"`
	NORP      float64 `json:"NORP"`
	ORG       float64 `json:"ORG"`
	PERSON    float64 `json:"PERSON"`
	PRODUCT   float64 `json:"PRODUCT"`
	EVENT     float64 `json:"EVENT"`
	WORKOFART float64 `json:"WORK_OF_ART"`
	LAW       float64 `json:"LAW"`
	LANGUAGE  float64 `json:"LANGUAGE"`
	DATE      float64 `json:"DATE"`
	TIME      float64 `json:"TIME"`
	PERCENT   float64 `json:"PERCENT"`
	MONEY     float64 `json:"MONEY"`
	QUANTITY  float64 `json:"QUANTITY"`
	ORDINAL   float64 `json:"ORDINAL"`
	CARDINAL  float64 `json:"CARDINAL"`
}

type PeopleVER struct {
	HASHCODE     int64
	IS_FIRSTNAME bool `json:"isFIRSTNAME"`
	IS_LASTNAME  bool `json:"isLASTNAME"`
	IS_STREET1   bool `json:"isSTREET1"`
	IS_STREET2   bool `json:"isSTREET2"`
	IS_CITY      bool `json:"isCITY"`
	IS_STATE     bool `json:"isSTATE"`
	IS_ZIPCODE   bool `json:"isZIPCODE"`
	IS_COUNTRY   bool `json:"isCOUNTRY"`
	IS_EMAIL     bool `json:"isEMAIL"`
	IS_PHONE     bool `json:"isPHONE"`
}

type InputColumn struct {
	NER       NER       `json:"NER"`
	PeopleERR PeopleERR `json:"ERR"`
	PeopleVER PeopleVER `json:"VER"`
	Name      string    `json:"Name"`
	Value     string    `json:"Value"`
	MatchKey  string    `json:"MK"`
}

type CityStateZip struct {
	Cities []string `json:"cities"`
	State  string   `json:"state"`
	Zip    string   `json:"zip"`
}

type NERCache struct {
	Columns      []NERcolumns `json:"columns"`
	TimeStamp    time.Time    `json:"time"`
	ApplyCounter int          `json:"counter"`
	Recompute    bool         `json:"dirty"`
	Source       string       `json:"source`
}

type NERcolumns struct {
	ColumnName  string             `json:"ColumnName"`
	NEREntities map[string]float64 `json:"NEREntities"`
}

type Output struct {
	Signature   Signature         `json:"signature"`
	Passthrough map[string]string `json:"passthrough"`
	Attributes  map[string]string `json:"attributes"`
	Prediction  Prediction        `json:"prediction`
	Columns     []InputColumn     `json:"columns`
}

type OutputFlag struct {
	People      bool
	Product     bool
	Campaign    bool
	Order       bool
	Consignment bool
	OrderDetail bool
	Event       bool
}

var ProjectID = os.Getenv("PROJECTID")

var PSPeople = os.Getenv("PSOUTPUTPEOPLE")
var PSEvent = os.Getenv("PSOUTPUTEVENT")
var PSProduct = os.Getenv("PSOUTPUTPRODUCT")
var PSCampaign = os.Getenv("PSOUTPUTCAMPAIGN")
var PSOrder = os.Getenv("PSOUTPUTORDER")
var PSConsignment = os.Getenv("PSOUTPUTCONSIGNMENT")
var PSOrderDetail = os.Getenv("PSOUTPUTORDERDETAIL")

var MLUrl = os.Getenv("PREDICTION")

var AIDataBucket = os.Getenv("AIBUCKET")
var RedisAddress = os.Getenv("MEMSTORE")

// global vars
var ctx context.Context
var ps *pubsub.Client
var topicPeople *pubsub.Topic
var topicEvent *pubsub.Topic
var topicProduct *pubsub.Topic
var topicCampaign *pubsub.Topic
var topicOrder *pubsub.Topic
var topicConsignment *pubsub.Topic
var topicOrderDetail *pubsub.Topic
var ai *ml.Service
var cs *storage.Client
var msPool *redis.Pool

var listLabels map[string]string
var listCities map[string]bool
var listStates map[string]bool
var listCountries map[string]bool
var listFirstNames map[string]bool
var listLastNames map[string]bool
var listError error
var listCityStateZip []CityStateZip

var reEmail = regexp.MustCompile("(?i)^[a-zA-Z0-9.!#$%&'*+/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$")
var rePhone = regexp.MustCompile(`(?i)^(?:(?:\(?(?:00|\+)([1-4]\d\d|[1-9]\d?)\)?)?[\-\.\ \\\/]?)?((?:\(?\d{1,}\)?[\-\.\ \\\/]?){0,})(?:[\-\.\ \\\/]?(?:#|ext\.?|extension|x)[\-\.\ \\\/]?(\d+))?$`)
var reZipcode = regexp.MustCompile(`(?i)^\d{5}(?:[-\s]\d{4})?$`)
var reStreet1 = regexp.MustCompile(`(?i)\d{1,4} [\w\s]{1,20}(?:street|st|avenue|ave|road|rd|highway|hwy|square|sq|trail|trl|drive|dr|court|ct|park|parkway|pkwy|circle|cir|boulevard|blvd)\W?`)
var reStreet2 = regexp.MustCompile(`(?i)apartment|apt|unit|box`)
var reGraduationYear = regexp.MustCompile(`20^\d{2}$`)
var reNumberOnly = regexp.MustCompile("[^0-9]+")
var reConcatenatedAddress = regexp.MustCompile(`(\d*)\s+((?:[\w+\s*\-])+)[\,]\s+([a-zA-Z]+)\s+([0-9a-zA-Z]+)`)
var reConcatenatedCityStateZip = regexp.MustCompile(`((?:[\w+\s*\-])+)[\,]\s+([a-zA-Z]+)\s+([0-9a-zA-Z]+)`)
var reResidenceHall = regexp.MustCompile(`(?i)\sALPHA|ALUMNI|APARTMENT|APTS|BETA|BUILDING|CAMPUS|CENTENNIAL|CENTER|CHI|COLLEGE|COMMON|COMMUNITY|COMPLEX|COURT|CROSS|DELTA|DORM|EPSILON|ETA|FOUNDER|FOUNTAIN|FRATERNITY|GAMMA|GARDEN|GREEK|HALL|HEIGHT|HERITAGE|HIGH|HILL|HOME|HONOR|HOUS|INN|INTERNATIONAL|IOTA|KAPPA|LAMBDA|LANDING|LEARNING|LIVING|LODGE|MEMORIAL|MU|NU|OMEGA|OMICRON|PARK|PHASE|PHI|PI|PLACE|PLAZA|PSI|RESIDEN|RHO|RIVER|SCHOLARSHIP|SIGMA|SQUARE|STATE|STUDENT|SUITE|TAU|TERRACE|THETA|TOWER|TRADITIONAL|UNIV|UNIVERSITY|UPSILON|VIEW|VILLAGE|VISTA|WING|WOOD|XI|YOUNG|ZETA`)
var reNewline = regexp.MustCompile(`\r?\n`)

var reCleanupDigitsOnly = regexp.MustCompile("[^a-zA-Z0-9]+")

func init() {
	ctx = context.Background()
	ps, _ = pubsub.NewClient(ctx, ProjectID)
	topicPeople = ps.Topic(PSPeople)
	topicEvent = ps.Topic(PSEvent)
	topicProduct = ps.Topic(PSProduct)
	topicCampaign = ps.Topic(PSCampaign)
	topicOrder = ps.Topic(PSOrder)
	topicConsignment = ps.Topic(PSConsignment)
	topicOrderDetail = ps.Topic(PSOrderDetail)

	ai, _ := ml.NewService(ctx)
	cs, _ = storage.NewClient(ctx)

	var maxConnections = 2
	msPool = redis.NewPool(func() (redis.Conn, error) {
		return redis.Dial("tcp", RedisAddress)
	}, maxConnections)

	// preload the lists
	listLabels = map[string]string{"0": "", "1": "AD1", "2": "AD2", "3": "CITY", "4": "COUNTRY", "5": "EMAIL", "6": "FNAME", "7": "LNAME", "8": "PHONE", "9": "STATE", "10": "ZIP"}

	var err error
	listCities, err = ReadJsonArray(ctx, cs, AIDataBucket, "data/cities.json")
	if err != nil {
		log.Fatalf("Failed to read json %v from bucket", "data/cities.json")
	} else {
		log.Printf("read %v values from %v", len(listCities), "data/cities.json")
	}

	listStates, err = ReadJsonArray(ctx, cs, AIDataBucket, "data/states.json")
	if err != nil {
		log.Fatalf("Failed to read json %v from bucket", "data/states.json")
	} else {
		log.Printf("read %v values from %v", len(listStates), "data/states.json")
	}

	listCountries, err = ReadJsonArray(ctx, cs, AIDataBucket, "data/countries.json")
	if err != nil {
		log.Fatalf("Failed to read json %v from bucket", "data/countries.json")
	} else {
		log.Printf("read %v values from %v", len(listCountries), "data/countries.json")
	}

	listFirstNames, err = ReadJsonArray(ctx, cs, AIDataBucket, "data/first_names.json")
	if err != nil {
		log.Fatalf("Failed to read json %v from bucket", "data/first_names.json")
	} else {
		log.Printf("read %v values from %v", len(listFirstNames), "data/first_names.json")
	}

	listLastNames, err = ReadJsonArray(ctx, cs, AIDataBucket, "data/last_names.json")
	if err != nil {
		log.Fatalf("Failed to read json %v from bucket", "data/last_names.json")
	} else {
		log.Printf("read %v values from %v", len(listLastNames), "data/last_names.json")
	}

	listCityStateZip, err = ReadCityStateZip(ctx, cs, AIDataBucket, "data/zip_city_state.json")
	if err != nil {
		log.Fatalf("Failed to read json %v from bucket", "data/zip_city_state.json")
	} else {
		log.Printf("read %v values from %v", len(listCityStateZip), "data/zip_city_state.json")
	}

	log.Printf("init completed, ai basepath %v, pubsub topic names: %v, %v, %v, %v, %v, %v, %v", ai.BasePath, topicPeople, topicEvent, topicProduct, topicCampaign, topicOrder, topicConsignment, topicOrderDetail)
}

func PreProcess(ctx context.Context, m PubSubMessage) error {
	var input Input
	if err := json.Unmarshal(m.Data, &input); err != nil {
		log.Fatalf("Unable to unmarshal message %v with error %v", string(m.Data), err)
	}

	columns := GetColumnsFromInput(input)
	var flags OutputFlag
	// run all ERRs

	// update the flags

	// look up NER and call ML if People
	var prediction Prediction

	if flags.People {
		PeopleNERKey := GetNERKey(input.Signature, GetMapKeys(input.Fields))
		PeopleNER := FindNER(PeopleNERKey)

		if len(PeopleNER.Columns) > 0 {
			// copy NER into the columns
			for _, column := range columns {
				for _, ner := range PeopleNER.Columns {
					if strings.EqualFold(column.Name, ner.ColumnName) {
						MapNER(column, ner.NEREntities)
					}
				}
			}
		}

		mlInput := BuildMLData(columns)
		mlJSON, _ := json.Marshal(mlInput)
		log.Printf("ML request %v", string(mlJSON))
		reqBody := &ml.GoogleApi__HttpBody{
			Data: string(mlJSON),
		}
		req := ml.GoogleCloudMlV1__PredictRequest{
			HttpBody: reqBody,
		}
		req.HttpBody.ContentType = "application/json"

		mlPredict := ai.Projects.Predict(MLUrl, &req)
		r, err := mlPredict.Context(ctx).Do()
		if err != nil {
			log.Fatalf("error calling mlService, %v", err)
			return nil
		}

		if err := json.NewDecoder(strings.NewReader(r.Data)).Decode(&prediction); err != nil {
			if _, ok := err.(*json.SyntaxError); ok {
				log.Fatalf("error decoding json, %v", string(r.Data))
			}
		}
		if len(prediction.Predictions) == 0 {
			log.Fatalf("unexpected prediction returned, %v", string(r.Data))
		}
	}

	// pub
	var output Output
	output.Signature = input.Signature
	output.Passthrough = input.Passthrough
	output.Columns = columns
	output.Attributes = input.Attributes
	output.Prediction = prediction

	outputJSON, _ := json.Marshal(output)

	if flags.People {
		PubMessage(topicPeople, outputJSON)
	}
	if flags.Product {
		PubMessage(topicProduct, outputJSON)
	}
	if flags.Event {
		PubMessage(topicEvent, outputJSON)
	}
	if flags.Campaign {
		PubMessage(topicCampaign, outputJSON)
	}
	if flags.Order {
		PubMessage(topicOrder, outputJSON)
	}
	if flags.Consignment {
		PubMessage(topicConsignment, outputJSON)
	}
	if flags.OrderDetail {
		PubMessage(topicOrderDetail, outputJSON)
	}

	return nil
}

func MapNER(column InputColumn, ner map[string]float64) {
	for k, v := range ner {
		switch k {
		case "FAC":
			column.NER.FAC = v
			break
		case "GPE":
			column.NER.GPE = v
			break
		case "LOC":
			column.NER.LOC = v
			break
		case "NORP":
			column.NER.NORP = v
			break
		case "ORG":
			column.NER.ORG = v
			break
		case "PERSON":
			column.NER.PERSON = v
			break
		case "PRODUCT":
			column.NER.PRODUCT = v
			break
		case "EVENT":
			column.NER.EVENT = v
			break
		case "WORKOFART":
			column.NER.WORKOFART = v
			break
		case "LAW":
			column.NER.LAW = v
			break
		case "LANGUAGE":
			column.NER.LANGUAGE = v
			break
		case "DATE":
			column.NER.DATE = v
			break
		case "TIME":
			column.NER.TIME = v
			break
		case "PERCENT":
			column.NER.PERCENT = v
			break
		case "MONEY":
			column.NER.MONEY = v
			break
		case "QUANTITY":
			column.NER.QUANTITY = v
			break
		case "ORDINAL":
			column.NER.ORDINAL = v
			break
		case "CARDINAL":
			column.NER.CARDINAL = v
			break

		}
	}
}

func PubMessage(topic *pubsub.Topic, data []byte) {
	psresult := topic.Publish(ctx, &pubsub.Message{
		Data: data,
	})
	psid, err := psresult.Get(ctx)
	_, err = psresult.Get(ctx)
	if err != nil {
		log.Fatalf("Could not pub to pubsub: %v", err)
	} else {
		log.Printf("pubbed to %v as message id %v: %v", topic, psid, string(data))
	}
}

func GetColumnsFromInput(input Input) []InputColumn {
	var columns []InputColumn

	for k, v := range input.Fields {
		column := InputColumn{
			Name:      k,
			Value:     v,
			PeopleERR: PeopleERR{},
			NER:       NER{},
			PeopleVER: PeopleVER{},
		}
		columns = append(columns, column)
	}
	return columns
}

func GetMapKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func GetMapKeysFromSlice(m map[string][]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func GetLowerCaseSorted(m []string) []string {
	var result []string
	for _, k := range m {
		result = append(result, strings.ToLower(k))
	}
	sort.Strings(result)
	return result
}

func GetNERKey(sig Signature, columns []string) string {
	// concatenate all columnn headers together, in lower case
	keys := strings.Join(GetLowerCaseSorted(columns[:]), "-")
	hasher := sha1.New()
	io.WriteString(hasher, keys)
	return fmt.Sprintf("ner:%v:%v:%v:%x", sig.OwnerID, strings.ToLower(sig.Source), strings.ToLower(sig.EventType), hasher.Sum(nil))
}

func FindNER(key string) NERCache {
	var ner NERCache
	ms := msPool.Get()
	s, err := redis.String(ms.Do("GET", key))
	if err == nil {
		json.Unmarshal([]byte(s), &ner)
	}
	return ner
}

func GetPeopleVER(column *InputColumn) PeopleVER {
	var val = strings.TrimSpace(column.Value)
	log.Printf("features values is %v", val)
	val = RemoveDiacritics(val)
	result := PeopleVER{
		HASHCODE:     int64(GetHash(val)),
		IS_FIRSTNAME: ContainsBool(listFirstNames, val),
		IS_LASTNAME:  ContainsBool(listLastNames, val),
		IS_STREET1:   reStreet1.MatchString(val),
		IS_STREET2:   reStreet2.MatchString(val),
		IS_CITY:      ContainsBool(listCities, val),
		IS_STATE:     ContainsBool(listStates, val),
		IS_ZIPCODE:   reZipcode.MatchString(val),
		IS_COUNTRY:   ContainsBool(listCountries, val),
		IS_EMAIL:     reEmail.MatchString(val),
		IS_PHONE:     rePhone.MatchString(val) && len(val) >= 10,
	}
	columnJ, _ := json.Marshal(result)
	log.Printf("current VER %v", string(columnJ))
	return result
}

func GetHash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func BuildMLData(columns []InputColumn) MLInput {
	var instances [][]float64
	for _, column := range columns {
		var instance []float64
		instance = append(instance, float64(column.PeopleERR.FirstName))
		instance = append(instance, float64(column.PeopleERR.LastName))
		instance = append(instance, float64(column.PeopleERR.MiddleName))
		instance = append(instance, float64(column.PeopleERR.Suffix))
		instance = append(instance, float64(column.PeopleERR.FullName))
		instance = append(instance, float64(column.PeopleERR.Address1))
		instance = append(instance, float64(column.PeopleERR.Address2))
		instance = append(instance, float64(column.PeopleERR.City))
		instance = append(instance, float64(column.PeopleERR.State))
		instance = append(instance, float64(column.PeopleERR.ZipCode))
		instance = append(instance, float64(column.PeopleERR.County))
		instance = append(instance, float64(column.PeopleERR.Country))
		instance = append(instance, float64(column.PeopleERR.Email))
		instance = append(instance, float64(column.PeopleERR.ParentEmail))
		instance = append(instance, float64(column.PeopleERR.Gender))
		instance = append(instance, float64(column.PeopleERR.Phone))
		instance = append(instance, float64(column.PeopleERR.ParentFirstName))
		instance = append(instance, float64(column.PeopleERR.ParentLastName))
		instance = append(instance, float64(column.PeopleERR.Birthday))
		instance = append(instance, float64(column.PeopleERR.Age))
		instance = append(instance, float64(column.PeopleERR.ParentName))
		instance = append(instance, float64(column.NER.PERSON))
		instance = append(instance, float64(column.NER.NORP))
		instance = append(instance, float64(column.NER.FAC))
		instance = append(instance, float64(column.NER.ORG))
		instance = append(instance, float64(column.NER.GPE))
		instance = append(instance, float64(column.NER.LOC))
		instance = append(instance, float64(column.NER.PRODUCT))
		instance = append(instance, float64(column.NER.EVENT))
		instance = append(instance, float64(column.NER.WORKOFART))
		instance = append(instance, float64(column.NER.LAW))
		instance = append(instance, float64(column.NER.LANGUAGE))
		instance = append(instance, float64(column.NER.DATE))
		instance = append(instance, float64(column.NER.TIME))
		instance = append(instance, float64(column.NER.PERCENT))
		instance = append(instance, float64(column.NER.MONEY))
		instance = append(instance, float64(column.NER.QUANTITY))
		instance = append(instance, float64(column.NER.ORDINAL))
		instance = append(instance, float64(column.NER.CARDINAL))
		instance = append(instance, float64(column.PeopleVER.HASHCODE))
		instance = append(instance, float64(ToUInt32(column.PeopleVER.IS_FIRSTNAME)))
		instance = append(instance, float64(ToUInt32(column.PeopleVER.IS_LASTNAME)))
		instance = append(instance, float64(ToUInt32(column.PeopleVER.IS_STREET1)))
		instance = append(instance, float64(ToUInt32(column.PeopleVER.IS_STREET2)))
		instance = append(instance, float64(ToUInt32(column.PeopleVER.IS_CITY)))
		instance = append(instance, float64(ToUInt32(column.PeopleVER.IS_STATE)))
		instance = append(instance, float64(ToUInt32(column.PeopleVER.IS_ZIPCODE)))
		instance = append(instance, float64(ToUInt32(column.PeopleVER.IS_COUNTRY)))
		instance = append(instance, float64(ToUInt32(column.PeopleVER.IS_EMAIL)))
		instance = append(instance, float64(ToUInt32(column.PeopleVER.IS_PHONE)))

		instances = append(instances, instance)
	}
	var mlInput MLInput
	mlInput.Instances = instances
	return mlInput
}

func ReadJsonArray(ctx context.Context, client *storage.Client, bucket, object string) (map[string]bool, error) {
	rc, err := client.Bucket(bucket).Object(object).NewReader(ctx)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	data, err := ioutil.ReadAll(rc)
	if err != nil {
		return nil, err
	}
	var intermediate []string
	json.Unmarshal(data, &intermediate)

	result := make(map[string]bool)
	for _, s := range intermediate {
		result[s] = true
	}
	return result, nil
}

func ReadJsonMap(ctx context.Context, client *storage.Client, bucket, object string) (map[string]string, error) {
	rc, err := client.Bucket(bucket).Object(object).NewReader(ctx)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	data, err := ioutil.ReadAll(rc)
	if err != nil {
		return nil, err
	}
	var result map[string]string
	json.Unmarshal(data, &result)

	return result, nil
}

func ReadCityStateZip(ctx context.Context, client *storage.Client, bucket, object string) ([]CityStateZip, error) {
	var result []CityStateZip
	rc, err := client.Bucket(bucket).Object(object).NewReader(ctx)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	data, err := ioutil.ReadAll(rc)
	if err != nil {
		return nil, err
	}
	json.Unmarshal(data, &result)
	return result, nil
}

func RemoveDiacritics(value string) string {
	t := transform.Chain(norm.NFD, transform.RemoveFunc(IsMn), norm.NFC)
	result, _, _ := transform.String(t, value)
	return result
}

func Contains(dict map[string]bool, key string) uint32 {
	if _, ok := dict[strings.ToUpper(key)]; ok {
		return 1
	}
	return 0
}

func ContainsBool(dict map[string]bool, key string) bool {
	if _, ok := dict[strings.ToUpper(key)]; ok {
		return true
	}
	return false
}

func ToUInt32(val bool) uint32 {
	if val {
		return 1
	}
	return 0
}

func IsMn(r rune) bool {
	return unicode.Is(unicode.Mn, r) // Mn: nonspacing marks
}
