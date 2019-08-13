package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"hash/fnv"
	"io/ioutil"
	"net/http"
	"regexp"
	"strings"
	"time"
	"unicode"

	"cloud.google.com/go/storage"
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/io/pubsubio"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
	"github.com/apache/beam/sdks/go/pkg/beam/transforms/stats"
	"github.com/apache/beam/sdks/go/pkg/beam/x/beamx"
	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"
	pb "google.golang.org/genproto/googleapis/pubsub/v1"
)

var PubsubTopic = "streamer-output-dev"
var ProjectID = "wemade-core"
var BucketData = "wemade-ai-platform"

var PredictionURL = "https://ml.googleapis.com/v1/projects/wemade-core/models/column_prediction_model:predict"

var listLabels map[string]string
var listCities map[string]bool
var listStates map[string]bool
var listCountries map[string]bool
var listFirstNames map[string]bool
var listLastNames map[string]bool

var reEmail = regexp.MustCompile("(?i)^[a-zA-Z0-9.!#$%&'*+/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$")
var rePhone = regexp.MustCompile(`(?i)^(?:(?:\(?(?:00|\+)([1-4]\d\d|[1-9]\d?)\)?)?[\-\.\ \\\/]?)?((?:\(?\d{1,}\)?[\-\.\ \\\/]?){0,})(?:[\-\.\ \\\/]?(?:#|ext\.?|extension|x)[\-\.\ \\\/]?(\d+))?$`)
var reZipcode = regexp.MustCompile(`(?i)^\d{5}(?:[-\s]\d{4})?$`)
var reStreet1 = regexp.MustCompile(`(?i)\d{1,4} [\w\s]{1,20}(?:street|st|avenue|ave|road|rd|highway|hwy|square|sq|trail|trl|drive|dr|court|ct|park|parkway|pkwy|circle|cir|boulevard|blvd)\W?`)
var reStreet2 = regexp.MustCompile(`(?i)apartment|apt|unit|box`)

type InputRecord struct {
	Columns []struct {
		ERR struct {
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
		} `json:"ERR"`
		NER struct {
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
		} `json:"NER"`
		Name  string `json:"Name"`
		Value string `json:"Value"`
	} `json:"Columns"`
	Owner     int    `json:"Owner"`
	Request   string `json:"Request"`
	Row       int    `json:"Row"`
	Source    string `json:"Source"`
	TimeStamp string `json:"TimeStamp"`
}

var httpClient *http.Client

const (
	MaxIdleConnections int = 20
	RequestTimeout     int = 5
)

func init() {
	httpClient = createHTTPClient()
}

// createHTTPClient for connection re-use
func createHTTPClient() *http.Client {
	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConnsPerHost: MaxIdleConnections,
		},
		Timeout: time.Duration(RequestTimeout) * time.Second,
	}

	return client
}

func readLines(ctx context.Context, client *storage.Client, bucket, object string) ([]string, error) {
	rc, err := client.Bucket(bucket).Object(object).NewReader(ctx)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	data, err := ioutil.ReadAll(rc)
	if err != nil {
		return nil, err
	}
	return strings.Split(string(data), "\n"), nil
}

func readJsonArray(ctx context.Context, client *storage.Client, bucket, object string) (map[string]bool, error) {
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

	var result map[string]bool
	for _, s := range intermediate {
		result[s] = true
	}
	return result, nil
}

func readJsonMap(ctx context.Context, client *storage.Client, bucket, object string) (map[string]string, error) {
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

func getHash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func isMn(r rune) bool {
	return unicode.Is(unicode.Mn, r) // Mn: nonspacing marks
}

func removeDiacritics(value string) string {
	t := transform.Chain(norm.NFD, transform.RemoveFunc(isMn), norm.NFC)
	result, _, _ := transform.String(t, value)
	return result
}

func contains(dict map[string]bool, key string) uint32 {
	if _, ok := dict[key]; ok {
		return 1
	}
	return 0
}

func toUInt32(val bool) uint32 {
	if val {
		return 1
	}
	return 0
}

func toZeroIfNotInMap(dict map[string]int, key string) float64 {
	if _, ok := dict[key]; ok {
		return float64(dict[key])
	}
	return 0
}

func getFeatures(value string) []uint32 {
	var val = strings.TrimSpace(value)
	val = removeDiacritics(val)
	var result []uint32
	result[0] = getHash(val)
	result[1] = contains(listFirstNames, val)
	result[2] = contains(listLastNames, val)
	result[3] = toUInt32(reStreet1.MatchString(val))
	result[4] = toUInt32(reStreet2.MatchString(val))
	result[5] = contains(listCities, val)
	result[6] = contains(listStates, val)
	result[7] = toUInt32(reZipcode.MatchString(val))
	result[8] = contains(listCountries, val)
	result[9] = toUInt32(reEmail.MatchString(val))
	result[10] = toUInt32(rePhone.MatchString(val))
	return result
}

func main() {
	ctx := context.Background()

	// If beamx or Go flags are used, flags must be parsed first.
	flag.Parse()

	//read the lists
	sClient, err := storage.NewClient(ctx)
	listLabels, err := readJsonMap(ctx, sClient, BucketData, "data/labels.json")
	if err != nil {
		log.Fatalf(ctx, "Failed to read json %v from bucket", "data/labels.json")
	} else {
		log.Infof(ctx, "read %v values from %v", len(listLabels), "data/labels.json")
	}

	listCities, err := readJsonArray(ctx, sClient, BucketData, "data/cities.json")
	if err != nil {
		log.Fatalf(ctx, "Failed to read json %v from bucket", "data/cities.json")
	} else {
		log.Infof(ctx, "read %v values from %v", len(listCities), "data/cities.json")
	}

	listStates, err := readJsonArray(ctx, sClient, BucketData, "data/states.json")
	if err != nil {
		log.Fatalf(ctx, "Failed to read json %v from bucket", "data/states.json")
	} else {
		log.Infof(ctx, "read %v values from %v", len(listStates), "data/states.json")
	}

	listCountries, err := readJsonArray(ctx, sClient, BucketData, "data/countries.json")
	if err != nil {
		log.Fatalf(ctx, "Failed to read json %v from bucket", "data/countries.json")
	} else {
		log.Infof(ctx, "read %v values from %v", len(listCountries), "data/countries.json")
	}

	listFirstNames, err := readJsonArray(ctx, sClient, BucketData, "data/first_names.json")
	if err != nil {
		log.Fatalf(ctx, "Failed to read json %v from bucket", "data/first_names.json")
	} else {
		log.Infof(ctx, "read %v values from %v", len(listFirstNames), "data/first_names.json")
	}

	listLastNames, err := readJsonArray(ctx, sClient, BucketData, "data/last_names.json")
	if err != nil {
		log.Fatalf(ctx, "Failed to read json %v from bucket", "data/last_names.json")
	} else {
		log.Infof(ctx, "read %v values from %v", len(listLastNames), "data/last_names.json")
	}

	// beam.Init() is an initialization hook that must called on startup. On
	// distributed runners, it is used to intercept control.
	beam.Init()

	p := beam.NewPipeline()
	s := p.Root()

	buildPipeline(ctx, s)

	if err := beamx.Run(ctx, p); err != nil {
		log.Fatalf(ctx, "Failed to execute job: %v", err)
	}
}

func buildPipeline(ctx context.Context, s beam.Scope) {
	// nothing -> PCollection<Painting>
	pubsubOptions := pubsubio.ReadOptions{WithAttributes: true}
	messages := pubsubio.Read(s, ProjectID, PubsubTopic, &pubsubOptions)

	// pre-process each record
	records := beam.ParDo(s, func(message pb.PubsubMessage, emit func(string)) {
		var input InputRecord
		err := json.Unmarshal(message.Data, &input)
		if err != nil {
			log.Warnf(ctx, "error decoding json, %v", string(message.Data))
		}
		var instances [][]float64
		for _, column := range input.Columns {
			var instance []float64
			instance = append(instance, float64(column.ERR.FirstName))
			instance = append(instance, float64(column.ERR.LastName))
			instance = append(instance, float64(column.ERR.MiddleName))
			instance = append(instance, float64(column.ERR.Suffix))
			instance = append(instance, float64(column.ERR.FullName))
			instance = append(instance, float64(column.ERR.Address1))
			instance = append(instance, float64(column.ERR.Address2))
			instance = append(instance, float64(column.ERR.City))
			instance = append(instance, float64(column.ERR.State))
			instance = append(instance, float64(column.ERR.ZipCode))
			instance = append(instance, float64(column.ERR.County))
			instance = append(instance, float64(column.ERR.Country))
			instance = append(instance, float64(column.ERR.Email))
			instance = append(instance, float64(column.ERR.ParentEmail))
			instance = append(instance, float64(column.ERR.Gender))
			instance = append(instance, float64(column.ERR.Phone))
			instance = append(instance, float64(column.ERR.ParentFirstName))
			instance = append(instance, float64(column.ERR.ParentLastName))
			instance = append(instance, float64(column.ERR.Birthday))
			instance = append(instance, float64(column.ERR.Age))
			instance = append(instance, float64(column.ERR.ParentName))
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

			features := getFeatures(column.Value)
			for _, feature := range features {
				instance = append(instance, float64(feature))
			}
			instances = append(instances, instance)
			reqJSON, _ := json.Marshal(instances)
			req, err := http.NewRequest("POST", PredictionURL, bytes.NewBuffer(reqJSON))
			if err != nil {
				log.Fatalf(ctx, "Error Occured. %+v", err)
			}
			req.Header.Set("Content-Type", "application/json")

			response, err := httpClient.Do(req)
			if err != nil && response == nil {
				log.Fatalf(ctx, "Error sending request to API endpoint. %+v", err)
			}
			// Close the connection to reuse it
			defer response.Body.Close()

			// Let's check if the work actually is done
			// We have seen inconsistencies even when we get 200 OK response
			body, err := ioutil.ReadAll(response.Body)
			if err != nil {
				log.Fatalf(ctx, "Couldn't parse response body. %+v", err)
			}
			emit(string(body))
		}
	}, messages)

	log.Infof(ctx, "PCollection size is %v", stats.Count(s, records))
	//preprocessing := beam.ParDo
}
