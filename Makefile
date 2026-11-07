# One set of commands in a Codespace and on a laptop. `make help` lists them.

PROFILE ?= core
DAY ?= 2026-11-03
ROWS ?= 200000
CHAOS ?=

PY := .venv/bin/python
COMPOSE := docker compose

# Every recipe sees .env, so dbt and the scripts use the same ports and passwords as compose.
-include .env
export
# core always runs; PROFILE=all starts every profile
PROFILE_FLAGS := --profile core $(if $(filter all,$(PROFILE)),--profile '*',$(if $(filter-out core,$(PROFILE)),--profile $(PROFILE)))

.DEFAULT_GOAL := help
.PHONY: help setup up down ps logs psql seed seed-week extract extract-week dbt demo doctor \
	check-w4 check-w5 check-w6 check-w7 check-w8 check-w9 test lint clean

help: ## List the targets
	@grep -hE '^[a-z0-9-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "}; {printf "  %-12s %s\n", $$1, $$2}'

.env:
	cp .env.example .env

# SSL_CERT_FILE: installing dbt-core downloads a wheel from GitHub, which fails on python.org
# Python for macOS (no CA bundle) unless pointed at the certificates pip already carries.
$(PY): pyproject.toml
	@test -x $(PY) || python3 -m venv .venv
	@export SSL_CERT_FILE="$$($(PY) -c 'import pip._vendor.certifi as c; print(c.where())')"; \
	if command -v uv >/dev/null 2>&1; then uv pip install --python $(PY) -e '.[dev]'; \
	else $(PY) -m pip install -q --upgrade pip && $(PY) -m pip install -q -e '.[dev]'; fi
	@touch $(PY)

setup: .env $(PY) ## Create .env and the Python venv
	@echo "venv ready: $(PY)"

up: .env ## Start a profile: make up PROFILE=core|orchestration|compute|serving|observe|all
	$(COMPOSE) $(PROFILE_FLAGS) up -d --wait

down: .env ## Stop every container (data volumes are kept)
	$(COMPOSE) --profile '*' down

ps: .env ## Show what is running
	$(COMPOSE) --profile '*' ps

logs: .env ## Follow logs: make logs SERVICE=redpanda
	$(COMPOSE) --profile '*' logs -f --tail 100 $(SERVICE)

psql: .env ## Open psql: make psql DB=owopay|dw
	$(COMPOSE) exec postgres psql -U owopay -d $(or $(DB),owopay)

seed: setup ## Seed one day: make seed DAY=2026-11-03 ROWS=200000 [CHAOS=duplicates,late,bad_records]
	$(PY) -m owopay.generate --day $(DAY) --rows $(ROWS) $(if $(CHAOS),--chaos $(CHAOS))

seed-week: setup ## Seed the course week: 2026-11-03 (200k rows), then 04-09 (100k each)
	$(PY) -m owopay.generate --day 2026-11-03 --rows 200000
	for d in 04 05 06 07 08 09; do $(PY) -m owopay.generate --day 2026-11-$$d --rows 100000 || exit 1; done

extract: setup ## Copy Lagos days into dw.raw: make extract DAY=2026-11-09 [LOOKBACK=2]
	$(PY) ingestion/batch/extract_to_dw.py --day $(DAY) --lookback $(or $(LOOKBACK),2)

extract-week: setup ## Copy the seven course days into dw.raw (09 and the six days before it)
	$(PY) ingestion/batch/extract_to_dw.py --day 2026-11-09 --lookback 6

dbt: setup ## Run dbt from the repo root: make dbt ARGS="build -s staging"
	.venv/bin/dbt $(ARGS) --project-dir dbt --profiles-dir dbt

demo: setup ## Print live counts from Postgres, the topics and the buckets
	$(PY) scripts/demo.py

doctor: .env ## Check this machine: RAM, disk, Docker, Compose, WSL2, ports, images
	@python3 scripts/doctor.py

check-w4: setup ## Week 4 checkpoints, headless
	$(PY) scripts/checks/check_w4.py

check-w5: setup ## Week 5 checkpoints, headless
	$(PY) scripts/checks/check_w5.py

check-w6 check-w7 check-w8 check-w9:
	@echo "$@ arrives with its week." && exit 1

test: setup ## Unit tests (pytest, DuckDB)
	$(PY) -m pytest -q

lint: setup ## Lint and format check (ruff)
	$(PY) -m ruff check . && $(PY) -m ruff format --check .

clean: .env ## Stop everything AND delete all data volumes
	@read -p "This deletes every database, topic and bucket. Type yes: " ok && [ "$$ok" = yes ]
	$(COMPOSE) --profile '*' down -v
