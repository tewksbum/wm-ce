package peoplepost

import (
	"fmt"
	"testing"
)

func TestParseName(t *testing.T) {
	parsed1 := ParseName("Jie Yang")
	parsed2 := ParseName("Wilson, Kuston A.")
	parsed3 := ParseName("Wilson, Kuston")
	fmt.Printf("%v, %v, %v", parsed1, parsed2, parsed3)

}
