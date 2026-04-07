package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"sort"
	"time"

	metadata "github.com/Athul0491/IceCore/gen/metadata"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	targetAddr = "127.0.0.1:50051"
	tableName  = "bench_events"
)

func main() {
	conn, err := grpc.NewClient(targetAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	client := metadata.NewMetadataServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	mustCreateTable(ctx, client)
	mustSeedSnapshot(ctx, client)

	fmt.Println("warming cache...")
	_, _ = client.GetPartitions(ctx, &metadata.PartitionRequest{
		TableName: tableName,
	})

	fmt.Println("running benchmark...")
	latencies := make([]time.Duration, 0, 1000)

	for i := 0; i < 1000; i++ {
		start := time.Now()
		resp, err := client.GetPartitions(ctx, &metadata.PartitionRequest{
			TableName: tableName,
		})
		elapsed := time.Since(start)
		if err != nil {
			log.Fatalf("GetPartitions failed: %v", err)
		}
		if len(resp.GetPartitions()) == 0 {
			log.Fatalf("expected partitions, got none")
		}
		latencies = append(latencies, elapsed)
	}

	printStats(latencies)
}

func mustCreateTable(ctx context.Context, client metadata.MetadataServiceClient) {
	_, err := client.CreateTable(ctx, &metadata.CreateTableRequest{
		TableName:     tableName,
		SchemaJson:    `{"fields":[{"name":"event_id","type":"long"},{"name":"ts","type":"timestamp"}]}`,
		PartitionSpec: "month",
		Properties: map[string]string{
			"owner": "bench",
		},
	})
	if err != nil {
		log.Fatalf("CreateTable: %v", err)
	}
}

func mustSeedSnapshot(ctx context.Context, client metadata.MetadataServiceClient) {
	// try to append a decent amount of partitions so DB path is non-trivial
	parts := make([]*metadata.PartitionInfo, 0, 200)
	for i := 0; i < 200; i++ {
		parts = append(parts, &metadata.PartitionInfo{
			PartitionKey: fmt.Sprintf("month=2025-%02d-part=%03d", (i%12)+1, i),
			DataFilePath: fmt.Sprintf("s3://bucket/bench_events/part-%03d.parquet", i),
			RowCount:     100000,
			SizeBytes:    15000000,
			FileFormat:   "parquet",
		})
	}

	// first snapshot only; if it already exists, that's okay
	_, _ = client.CommitSnapshot(ctx, &metadata.SnapshotRequest{
		TableName:        tableName,
		ParentSnapshotId: 0,
		Operation:        "append",
		NewPartitions:    parts,
	})
}

func printStats(latencies []time.Duration) {
	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	var total time.Duration
	for _, l := range latencies {
		total += l
	}

	avg := total / time.Duration(len(latencies))
	p50 := percentile(latencies, 0.50)
	p95 := percentile(latencies, 0.95)
	p99 := percentile(latencies, 0.99)

	fmt.Printf("requests: %d\n", len(latencies))
	fmt.Printf("avg: %s\n", avg)
	fmt.Printf("p50: %s\n", p50)
	fmt.Printf("p95: %s\n", p95)
	fmt.Printf("p99: %s\n", p99)
	fmt.Printf("min: %s\n", latencies[0])
	fmt.Printf("max: %s\n", latencies[len(latencies)-1])
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * p)
	return sorted[idx]
}

// silence unused import issue on some setups
var _ net.Conn
