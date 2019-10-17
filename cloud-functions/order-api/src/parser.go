package orderapi

import (
	"github.com/tidwall/gjson"
)

func parseOrderJSON(orderjson string) error {
	address := gjson.Get(orderjson, "Address")
	pubMessage([]byte(address.Raw))
	consignments := gjson.Get(orderjson, "Consignments")
	for _, c := range consignments.Array() {
		pubMessage([]byte(c.Raw))
		a := c.Get("Address")
		pubMessage([]byte(a.Raw))
		v := c.Get("Vendor")
		pubMessage([]byte(v.Raw))
		shipments := c.Get("Shipments")
		for _, s := range shipments.Array() {
			si := s.Get("ShipItems")
			for _, i := range si.Array() {
				pubMessage([]byte(i.Raw))
				ssi := i.Get("ShipSubItems")
				for _, ii := range ssi.Array() {
					pubMessage([]byte(ii.Raw))
				}
			}
		}
	}
	students := gjson.Get(orderjson, "Students")
	pubMessage([]byte(students.Raw))
	return nil
}
