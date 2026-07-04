.PHONY: help build test tidy up down topics logs console \
        feed-adapter ingestor batch-publisher chain-submitter tradegen \
        consume-trades consume-batches consume-dlq pipeline-up

help:
	@echo "Infra:"
	@echo "  make up             - start Redpanda + Console UI + create topics (host demo mode)"
	@echo "  make console        - open the Redpanda Console UI in the browser"
	@echo "  make down           - stop and remove all compose resources"
	@echo "  make topics         - (re)create the four topics"
	@echo "  make pipeline-up    - run the full pipeline in containers"
	@echo "Build/test:"
	@echo "  make build          - go build ./..."
	@echo "  make test           - go test ./..."
	@echo "Run services on host (need 'make up' first):"
	@echo "  make feed-adapter | ingestor | batch-publisher | chain-submitter | tradegen"
	@echo "Inspect topics:"
	@echo "  make consume-trades | consume-batches | consume-dlq"

build:
	go build ./...

test:
	go test ./...

tidy:
	go mod tidy

up:
	docker compose up -d redpanda topic-init console
	@echo "Redpanda is up on localhost:19092 (host) / redpanda:9092 (containers)"
	@echo "Console UI: http://localhost:8080"

console:
	@echo "Opening http://localhost:8080 ..."
	@open http://localhost:8080 2>/dev/null || xdg-open http://localhost:8080 2>/dev/null || echo "Open http://localhost:8080 in your browser"

pipeline-up:
	docker compose --profile services up --build

down:
	docker compose down -v

topics:
	./scripts/create-topics.sh

logs:
	docker compose logs -f redpanda

feed-adapter:
	go run ./cmd/feed-adapter

ingestor:
	go run ./cmd/ingestor

batch-publisher:
	go run ./cmd/batch-publisher

chain-submitter:
	go run ./cmd/chain-submitter

tradegen:
	go run ./cmd/tradegen

consume-trades:
	docker exec -it redpanda rpk topic consume trades

consume-batches:
	docker exec -it redpanda rpk topic consume settlement_batches

consume-dlq:
	docker exec -it redpanda rpk topic consume dead_letter
