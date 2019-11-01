package bq

import "testing"

func TestWrite(t *testing.T) {
	type args struct {
		projectID string
		datasetID string
		tableID   string
		obj       interface{}
	}
	type introspace struct {
		ID   int
		Name string
		Age  int
	}
	poo := introspace{
		ID:   108,
		Name: "cmel",
		Age:  99,
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "BQ write test",
			args: args{
				projectID: "wemade-core",
				datasetID: "5648883367542784",
				tableID:   "event",
				obj:       poo,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := Write(tt.args.projectID, tt.args.datasetID, tt.args.tableID, tt.args.obj); (err != nil) != tt.wantErr {
				t.Errorf("Write() error = %#v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
