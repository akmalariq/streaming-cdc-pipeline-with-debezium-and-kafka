COMPOSE := docker compose -f infra/docker-compose.yml

.PHONY: help sync up down logs ps register-connector simulate consume verify test lint fmt psql

help:
	@echo "sync               install dependencies (uv sync --dev)"
	@echo "up                 start postgres, redpanda, debezium connect, clickhouse"
	@echo "down               stop the stack and delete volumes"
	@echo "logs               tail stack logs"
	@echo "register-connector POST the Debezium Postgres connector to Kafka Connect"
	@echo "simulate           produce simulated change events into Kafka"
	@echo "consume            consume events into ClickHouse (bounded run)"
	@echo "verify             report ClickHouse sink row counts"
	@echo "test               run the offline test suite"
	@echo "lint               ruff check"
	@echo "psql               open a psql shell on the source database"

sync:
	uv sync --dev

up:
	$(COMPOSE) up -d --wait

down:
	$(COMPOSE) down -v

logs:
	$(COMPOSE) logs -f --tail=50

ps:
	$(COMPOSE) ps

register-connector:
	uv run cdc register-connector

simulate:
	uv run cdc simulate --count 25

consume:
	uv run cdc consume --max-messages 50 --idle-timeout 10

verify:
	uv run cdc verify --optimize

test:
	uv run pytest -q

lint:
	uv run ruff check .

fmt:
	uv run ruff check . --fix

psql:
	docker exec -it cdc-postgres psql -U cdc_user -d shopdb
