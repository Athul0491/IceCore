package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/Athul0491/IceCore/gen/metadata"
	"github.com/Athul0491/IceCore/internal/config"
	"github.com/Athul0491/IceCore/internal/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	cfg := config.FromEnv()

	log.Println("[IceLog] Metadata Control Plane")
	log.Printf("  gRPC address : %s\n", cfg.GRPCAddress)
	log.Printf("  PG pool size : %d\n", cfg.PoolSize)
	log.Printf("  Cache cap    : %d\n", cfg.CacheCapacity)
	log.Printf("  Txn timeout  : %s\n", cfg.TxnTimeout)

	svc, err := server.New(cfg)
	if err != nil {
		log.Fatalf("failed to initialize metadata server: %v", err)
	}
	defer svc.Close()

	lis, err := net.Listen("tcp", cfg.GRPCAddress)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", cfg.GRPCAddress, err)
	}

	grpcServer := grpc.NewServer()
	metadata.RegisterMetadataServiceServer(grpcServer, svc)
	reflection.Register(grpcServer)

	errCh := make(chan error, 1)
	go func() {
		log.Printf("[main] server listening on %s\n", cfg.GRPCAddress)
		errCh <- grpcServer.Serve(lis)
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-stop:
		log.Printf("[main] received signal %s, shutting down...\n", sig)
		grpcServer.GracefulStop()
	case err := <-errCh:
		if err != nil {
			log.Fatalf("gRPC server stopped with error: %v", err)
		}
	}

	_ = context.Background()
	log.Println("[main] server stopped")
}
