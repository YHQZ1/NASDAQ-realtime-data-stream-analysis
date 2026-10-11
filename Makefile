PYTHON := .venv/bin/python
RUFF := .venv/bin/ruff
SPEED ?= 60

.PHONY: venv up down download train evaluate stream produce reset format lint

venv:
	python3 -m venv .venv
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install -e ".[dev]"

up:
	docker compose up -d --wait

down:
	docker compose down

download:
	$(PYTHON) -m nasdaq_stream.ingest.download

train:
	$(PYTHON) -m nasdaq_stream.ml.train

evaluate:
	$(PYTHON) -m nasdaq_stream.ml.evaluate

stream:
	$(PYTHON) -m nasdaq_stream.streaming.job

produce:
	$(PYTHON) -m nasdaq_stream.ingest.producer --speed $(SPEED)

reset:
	rm -rf checkpoints
	docker compose exec -T postgres psql -U market -d market -c "TRUNCATE raw_bars, agg_5m, volatility, anomaly_scores, pipeline_metrics"
	docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic ticks.raw || true

format:
	$(RUFF) check --fix src
	$(RUFF) format src

lint:
	$(RUFF) check src
	$(RUFF) format --check src
