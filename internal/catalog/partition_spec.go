package catalog

// PartitionTransform is the set of supported Iceberg partition transforms.
type PartitionTransform string

const (
	TransformIdentity PartitionTransform = "identity"
	TransformYear     PartitionTransform = "year"
	TransformMonth    PartitionTransform = "month"
	TransformDay      PartitionTransform = "day"
	TransformHour     PartitionTransform = "hour"
	TransformBucket   PartitionTransform = "bucket"
	TransformTruncate PartitionTransform = "truncate"
)

var validTransforms = map[PartitionTransform]bool{
	TransformIdentity: true,
	TransformYear:     true,
	TransformMonth:    true,
	TransformDay:      true,
	TransformHour:     true,
	TransformBucket:   true,
	TransformTruncate: true,
}

// paramRequired returns true for transforms that require a numeric parameter.
func paramRequired(t PartitionTransform) bool {
	return t == TransformBucket || t == TransformTruncate
}

// PartitionField mirrors an Iceberg PartitionField.
// FieldID must be unique within a spec and stable across evolution.
type PartitionField struct {
	FieldID        int                `json:"field_id"`
	SourceColumn   string             `json:"source_column"`
	Transform      PartitionTransform `json:"transform"`
	TransformParam *int               `json:"transform_param,omitempty"`
}

// PartitionSpec is what gets stored in partition_specs.spec_json and tables.partition_spec.
type PartitionSpec struct {
	Fields []PartitionField `json:"fields"`
}
