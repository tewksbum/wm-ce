package preprocess

import (
	"encoding/json"
	"fmt"
	"testing"
)

// DONT FORGET TO COMMENT OUT THE FATALS ON INIT() ON THE PRE-PROCESS FILE

func TestSignatures(t *testing.T) {
	perr := GetPeopleERR("MAILING STREET")
	j, _ := json.Marshal(perr)
	fmt.Printf("%v", string(j))
}
