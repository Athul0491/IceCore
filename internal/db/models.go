package db

type PartitionRow struct {
	PartitionID     int64
	TableID         int64
	SnapshotID      int64
	PartitionKey    string
	DataFilePath    string
	FileFormat      string
	RowCount        int64
	SizeBytes       int64
	ColumnStatsJSON string
}

type TableRow struct {
	TableID           int64
	TableName         string
	SchemaJSON        string
	SchemaVersion     int32
	PartitionSpec     string
	CurrentSnapshotID int64
	PropertiesJSON    string
}

type SnapshotRow struct {
	SnapshotID         int64
	TableID            int64
	ParentSnapshotID   int64
	Operation          string
	AddedFilesCount    int32
	DeletedFilesCount  int32
	CommittedAt        string
}

type TransactionRow struct {
	TxnID          int64
	ClientID       string
	ReadSnapshotID int64
	Status         string
	IsolationLevel string
	StartedAt      string
	CommittedAt    *string
}