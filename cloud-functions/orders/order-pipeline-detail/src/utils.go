package orderpipeline

import (
	"encoding/json"
	"hash/fnv"
	"log"
	"regexp"
	"strings"
	"unicode"

	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"
)

var (
	reTerms = regexp.MustCompile(`visa|amex|american.*express|master.*card|discover|net.?\d{0,2}`)
)

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
