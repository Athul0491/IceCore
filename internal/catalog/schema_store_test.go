package catalog

import "testing"

func TestValidateSchemaChange(t *testing.T) {
	store := &SchemaStore{}

	tests := []struct {
		name     string
		proposed string
		wantErr  string
	}{
		{
			name:     "empty",
			proposed: "",
			wantErr:  "Proposed schema cannot be empty",
		},
		{
			name:     "empty object",
			proposed: "{}",
			wantErr:  "Proposed schema cannot be empty",
		},
		{
			name:     "empty object with whitespace",
			proposed: "  {}  ",
			wantErr:  "Proposed schema cannot be empty",
		},
		{
			name:     "not json",
			proposed: "fields",
			wantErr:  "Proposed schema must be valid JSON object",
		},
		{
			name:     "array",
			proposed: `[]`,
			wantErr:  "Proposed schema must be valid JSON object",
		},
		{
			name:     "malformed object",
			proposed: `{bad}`,
			wantErr:  "Proposed schema must be valid JSON object",
		},
		{
			name:     "valid object",
			proposed: `{"fields":[{"name":"event_id","type":"long"}]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := store.ValidateSchemaChange(`{"fields":[]}`, tt.proposed)
			if got != tt.wantErr {
				t.Fatalf("expected %q, got %q", tt.wantErr, got)
			}
		})
	}
}
