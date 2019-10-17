package orderapi

import "testing"

func Test_parseOrderJSON(t *testing.T) {
	type args struct {
		orderjson string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "parseOrderJSON OK",
			args: args{
				orderjson: orderJSON,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := parseOrderJSON(tt.args.orderjson); (err != nil) != tt.wantErr {
				t.Errorf("parseOrderJSON() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

var (
	orderJSON = `{
		"Order": "Paste an OCM order here"
	}`
)
