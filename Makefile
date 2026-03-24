PROTO_DIR=proto
PROTO_FILE=$(PROTO_DIR)/metadata_service.proto

proto:
	protoc \
		--proto_path=$(PROTO_DIR) \
		--go_out=. \
		--go-grpc_out=. \
		$(PROTO_FILE)

run:
	go run ./cmd/server

tidy:
	go mod tidy