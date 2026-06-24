package catalog

import "testing"

func ptr(v int) *int { return &v }

func TestValidateSpecChange(t *testing.T) {
	store := &PartitionSpecStore{}

	tests := []struct {
		name     string
		current  string
		proposed string
		wantErr  string
	}{
		{
			name:     "invalid JSON",
			proposed: "not-json",
			wantErr:  "proposed partition spec is not valid JSON",
		},
		{
			name:     "empty fields array",
			proposed: `{"fields":[]}`,
			wantErr:  "proposed partition spec must have at least one field",
		},
		{
			name:     "missing source_column",
			proposed: `{"fields":[{"field_id":1,"source_column":"","transform":"identity"}]}`,
			wantErr:  "field at index 0 has empty source_column",
		},
		{
			name:     "unknown transform",
			proposed: `{"fields":[{"field_id":1,"source_column":"col","transform":"hash"}]}`,
			wantErr:  `field "col" has unknown transform "hash"`,
		},
		{
			name:     "bucket missing param",
			proposed: `{"fields":[{"field_id":1,"source_column":"col","transform":"bucket"}]}`,
			wantErr:  `field "col" transform "bucket" requires a positive transform_param`,
		},
		{
			name:     "truncate missing param",
			proposed: `{"fields":[{"field_id":1,"source_column":"col","transform":"truncate"}]}`,
			wantErr:  `field "col" transform "truncate" requires a positive transform_param`,
		},
		{
			name:     "identity with param",
			proposed: `{"fields":[{"field_id":1,"source_column":"col","transform":"identity","transform_param":10}]}`,
			wantErr:  `field "col" transform "identity" must not have transform_param`,
		},
		{
			name:     "duplicate field_ids",
			proposed: `{"fields":[{"field_id":1,"source_column":"a","transform":"identity"},{"field_id":1,"source_column":"b","transform":"identity"}]}`,
			wantErr:  "duplicate field_id 1",
		},
		{
			name:     "valid single identity field, no current",
			proposed: `{"fields":[{"field_id":1,"source_column":"month","transform":"identity"}]}`,
			wantErr:  "",
		},
		{
			name:     "valid bucket with param",
			proposed: `{"fields":[{"field_id":1,"source_column":"id","transform":"bucket","transform_param":16}]}`,
			wantErr:  "",
		},
		{
			name:     "remove existing field",
			current:  `{"fields":[{"field_id":1,"source_column":"month","transform":"identity"},{"field_id":2,"source_column":"region","transform":"identity"}]}`,
			proposed: `{"fields":[{"field_id":1,"source_column":"month","transform":"identity"}]}`,
			wantErr:  "field_id 2",
		},
		{
			name:     "change source_column",
			current:  `{"fields":[{"field_id":1,"source_column":"month","transform":"identity"}]}`,
			proposed: `{"fields":[{"field_id":1,"source_column":"year","transform":"identity"}]}`,
			wantErr:  "cannot change source_column",
		},
		{
			name:     "change transform",
			current:  `{"fields":[{"field_id":1,"source_column":"ts","transform":"identity"}]}`,
			proposed: `{"fields":[{"field_id":1,"source_column":"ts","transform":"month"}]}`,
			wantErr:  "cannot change transform",
		},
		{
			name:     "add field with lower field_id",
			current:  `{"fields":[{"field_id":2,"source_column":"month","transform":"identity"}]}`,
			proposed: `{"fields":[{"field_id":2,"source_column":"month","transform":"identity"},{"field_id":1,"source_column":"region","transform":"identity"}]}`,
			wantErr:  "must be greater than all existing field_ids",
		},
		{
			name:    "valid: add new field",
			current: `{"fields":[{"field_id":1,"source_column":"month","transform":"identity"}]}`,
			proposed: `{"fields":[` +
				`{"field_id":1,"source_column":"month","transform":"identity"},` +
				`{"field_id":2,"source_column":"region","transform":"identity"}` +
				`]}`,
			wantErr: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := store.ValidateSpecChange(tt.current, tt.proposed)
			if tt.wantErr == "" {
				if got != "" {
					t.Fatalf("expected no error, got %q", got)
				}
			} else {
				if got == "" {
					t.Fatalf("expected error containing %q, got empty string", tt.wantErr)
				}
				if len(got) > 0 && !containsString(got, tt.wantErr) {
					t.Fatalf("expected error containing %q, got %q", tt.wantErr, got)
				}
			}
		})
	}
}

func containsString(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(sub) == 0 ||
		func() bool {
			for i := 0; i <= len(s)-len(sub); i++ {
				if s[i:i+len(sub)] == sub {
					return true
				}
			}
			return false
		}())
}
