package db

// ColumnStats holds per-column bounds for one data file.
// Field naming follows Apache Iceberg DataFile field IDs:
//
//	value_count → 112, null_count → 113, min_value → 115,
//	max_value   → 116, size_bytes → 117, nan_count  → 128.
type ColumnStats struct {
	ColumnID   int32  `json:"column_id"`
	NullCount  int64  `json:"null_count"`
	NaNCount   int64  `json:"nan_count"`
	ValueCount int64  `json:"value_count"`
	MinValue   string `json:"min_value,omitempty"`
	MaxValue   string `json:"max_value,omitempty"`
	SizeBytes  int64  `json:"size_bytes"`
}

type PartitionRow struct {
	PartitionID  int64
	TableID      int64
	SnapshotID   int64
	PartitionKey string
	DataFilePath string
	FileFormat   string
	RowCount     int64
	SizeBytes    int64
	ColumnStats  []ColumnStats
}

type TableRow struct {
	TableID              int64
	TableName            string
	SchemaJSON           string
	SchemaVersion        int32
	PartitionSpec        string // stored as JSON text
	PartitionSpecVersion int32
	CurrentSnapshotID    int64
	PropertiesJSON       string
}

type PartitionSpecRow struct {
	PartitionSpecID int64
	TableID         int64
	SpecVersion     int32
	SpecJSON        string
	ChangedAt       string
	ChangeSummary   string
}

type SnapshotRow struct {
	SnapshotID        int64
	TableID           int64
	ParentSnapshotID  int64
	Operation         string
	AddedFilesCount   int32
	DeletedFilesCount int32
	CommittedAt       string
}

type TransactionRow struct {
	TxnID          int64
	ClientID       string
	TableID        *int64
	ReadSnapshotID int64
	Status         string
	IsolationLevel string
	StartedAt      string
	CommittedAt    *string
}

type ManifestFileRow struct {
	ManifestFileID     int64
	TableID            int64
	SnapshotID         int64
	ManifestPath       string
	PartitionSpecID    int32
	AddedFilesCount    int32
	DeletedFilesCount  int32
	AddedRowsCount     int64
	DeletedRowsCount   int64
	PartitionSummaries string // raw JSONB as text
}

type ManifestListRow struct {
	ManifestListID int64
	SnapshotID     int64
	TableID        int64
	ManifestCount  int32
}
