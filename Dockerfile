FROM golang:1.23-alpine AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /out/icelog-server \
    ./cmd/server

FROM alpine:3.21

RUN addgroup -S icelog && adduser -S -G icelog icelog

WORKDIR /app

COPY --from=build /out/icelog-server /app/icelog-server

USER icelog

EXPOSE 50051

ENTRYPOINT ["/app/icelog-server"]
