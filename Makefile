PROTO_DIR=proto
PROTO_FILE=$(PROTO_DIR)/metadata_service.proto
UNIT_PACKAGES=./cmd/... ./gen/... ./internal/...

.PHONY: proto run test test-unit test-integration tidy

proto:
	protoc \
		--proto_path=$(PROTO_DIR) \
		--go_out=. \
		--go-grpc_out=. \
		$(PROTO_FILE)
	cp -r gen/github.com/Athul0491/IceCore/gen/metadata/. gen/metadata/
	rm -rf gen/github.com github.com

run:
	go run ./cmd/server

test: test-unit test-integration

test-unit:
	go test -v $(UNIT_PACKAGES)

test-integration:
	docker compose up -d --wait postgres
	go test -v ./tests

tidy:
	go mod tidy
