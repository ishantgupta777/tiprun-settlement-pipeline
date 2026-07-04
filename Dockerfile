# Multi-stage build producing all four service binaries + the tradegen harness.
# The final image selects which binary to run via the compose `command`.
FROM golang:1.25 AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -o /out/feed-adapter ./cmd/feed-adapter \
 && CGO_ENABLED=0 go build -o /out/ingestor ./cmd/ingestor \
 && CGO_ENABLED=0 go build -o /out/batch-publisher ./cmd/batch-publisher \
 && CGO_ENABLED=0 go build -o /out/chain-submitter ./cmd/chain-submitter \
 && CGO_ENABLED=0 go build -o /out/tradegen ./cmd/tradegen

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ /usr/local/bin/
# Default command; overridden per-service in docker-compose.yml.
ENTRYPOINT ["/usr/local/bin/feed-adapter"]
