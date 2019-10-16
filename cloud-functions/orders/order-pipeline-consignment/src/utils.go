package orderpipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"hash/fnv"
	"io/ioutil"
	"log"
	"regexp"
	"strings"
	"unicode"

	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/google/uuid"
)

var (
	reTerms = regexp.MustCompile(`visa|amex|american.*express|master.*card|discover|net.?\d{0,2}`)
)

var (
	// These should be ENV vars
	esAddress  = "http://104.198.136.122:9200"
	esUser     = "elastic"
	esPassword = "TsLv8BtM"
)

func esLog(ctx *context.Context, body *bytes.Reader) {
	cfg := elasticsearch.Config{
		Addresses: []string{
			esAddress,
		},
		Username: esUser,
		Password: esPassword,
	}
	es, err := elasticsearch.NewClient(cfg)
	docID := uuid.New().String()
	req := esapi.IndexRequest{
		Index:        "order",
		DocumentType: "record",
		DocumentID:   docID,
		Body:         body,
		Refresh:      "true",
	}

	res, err := req.Do(*ctx, es)
	if err != nil {
		log.Fatalf("Error getting response: %s", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		resB, _ := ioutil.ReadAll(res.Body)
		log.Printf("[%s] Error indexing document ID=%v, Message=%v", res.Status(), docID, string(resB))
	} else {
		resB, _ := ioutil.ReadAll(res.Body)
		log.Printf("[%s] document ID=%v, Message=%v", res.Status(), docID, string(resB))
	}
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

func getVER(column *InputColumn) InputVER {
	var val = strings.ToLower(strings.TrimSpace(column.Value))
	log.Printf("features values is %v", val)
	val = removeDiacritics(val)
	result := InputVER{
		Hashcode: int64(getHash(val)),
	}
	columnJ, _ := json.Marshal(result)
	log.Printf("current VER %v", string(columnJ))
	return result
}
