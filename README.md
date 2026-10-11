# Real-Time Stock Market Analysis of NASDAQ Equities

A streaming analytics pipeline that replays one-minute NASDAQ bars through **Apache Kafka**,
processes them with **Spark Structured Streaming**, scores every bar with a **Spark MLlib**
anomaly detector, stores results in **PostgreSQL** and visualises them live in **Grafana**.

Case study #16, Big Data Analytics (CA-3), Symbiosis Institute of Technology, Pune.

## Architecture

```
yfinance ──► Parquet cache ──► Producer (replay, 1x-unthrottled) ──► Kafka  ticks.raw (3 partitions)
                                                                        │
                                              Spark Structured Streaming (local[*])
                                   ┌────────────────┬──────────────┬─────────────────┐
                                   ▼                ▼              ▼                 ▼
                               raw_bars         agg_5m        volatility       anomaly_scores
                             (validated)     (5-min OHLC,   (15-min sliding   (MLlib KMeans +
                                               VWAP)         return std)       z-score baseline)
                                   └────────────────┴──────────────┴─────────────────┘
                                                        │  foreachBatch / JDBC
                                                        ▼
                                                   PostgreSQL ──► Grafana
                                                        ▲
                                         pipeline_metrics (StreamingQueryListener)
```

| Layer | Technology | Role |
|---|---|---|
| Source | `yfinance` | 1-minute OHLCV bars for 50 NASDAQ tickers, last 7 trading days |
| Ingest | Python, `confluent-kafka` | Replays bars in event-time order at a configurable speed |
| Broker | Apache Kafka 3.9 (KRaft) | Topic `ticks.raw`, keyed by symbol so each ticker stays ordered |
| Processing | PySpark 4.0 Structured Streaming | Four concurrent queries with event-time windows and watermarks |
| ML | Spark MLlib | `VectorAssembler` + `KMeans` pipeline, trained offline and served in the stream |
| Storage | PostgreSQL 16 | One table per query, written with `foreachBatch` over JDBC |
| Visualisation | Grafana 11 | Provisioned datasource and a 13-panel dashboard |
| Operations | Docker Compose, Kafka UI | Broker, database, Grafana and topic inspection |

## Requirements

- Docker with Compose v2
- Java 17
- Python 3.11 or newer
- About 6 GB of free RAM (3 GB for the Spark driver, 2-3 GB for the containers)

## Quick start

```bash
make venv
make up
make download
make train
make evaluate
```

Then run the pipeline in two terminals:

```bash
make stream
make produce SPEED=600
```

Open Grafana at <http://localhost:3000> (anonymous viewer access is enabled; `admin` / `admin`
to edit). Kafka UI is at <http://localhost:8080> and the Spark UI at <http://localhost:4040>
while the stream is running.

`SPEED` is a multiplier over market time: `60` sends one market minute per second, `600` replays the
full week in about 4.5 minutes and `0` is unthrottled. Run `make reset` between replays to clear
checkpoints, tables and the topic.

## Project layout

```
src/nasdaq_stream/
  config.py              environment-driven settings
  spark_session.py       SparkSession factory (Kafka and JDBC packages, UTC, local filesystem)
  ingest/
    symbols.py           the 50-ticker universe
    download.py          yfinance download and Parquet cache
    producer.py          timestamp-ordered Kafka replay
  streaming/
    schema.py            message schema
    transforms.py        parsing, 5-minute candles, sliding volatility
    sinks.py             foreachBatch JDBC writers and query startup
    metrics.py           StreamingQueryListener writing pipeline_metrics
    job.py               entry point
  ml/
    features.py          per-symbol, per-half-hour z-score features
    model.py             trimmed KMeans training, scoring, persistence
    train.py             training entry point with a one-day holdout
    evaluate.py          injected-anomaly evaluation
sql/init.sql             PostgreSQL schema
grafana/                 provisioned datasource and dashboard
docker-compose.yml
```

## Design notes

**Replay with rebased timestamps.** NASDAQ is closed outside trading hours and yfinance only serves
one-minute data for roughly the last week, so the producer replays cached bars. Timestamps are
shifted by whole days so the final session lands on yesterday, which keeps the dashboard's relative
time ranges valid on any run date and preserves time-of-day and the weekend gap.

**Event-time windows with watermarks.** The 5-minute candle query (`min_by` and `max_by` for open and
close, volume-weighted average price) and the 15-minute sliding volatility query both use a 2-minute
watermark and append mode, so each window is written exactly once when it closes.

**Anomaly model.** Each bar is described by three features: absolute return, high-low range and log
volume. All three are standardised against the same symbol's statistics for the same half-hour of the
day, which removes the intraday volume curve and per-ticker volatility differences. A trimmed KMeans
(k = 4, refit after dropping the farthest 1% of training points) learns the normal behaviour, and the
anomaly score is the distance to the nearest centre, thresholded at the 99.5th percentile of
training distances. A univariate return z-score rule is scored alongside it as a baseline.

**No stateful ML in the stream.** Baseline statistics are a static broadcast table joined to the
stream, so scoring needs no per-key state and restarts cleanly from checkpoints.

## Results

Measured on a 16 GB laptop, Spark in local mode, containers on the same machine.

| Metric | Value |
|---|---|
| Bars processed | 135,028 across 50 symbols |
| 5-minute candles produced | 27,249 |
| Sliding volatility windows | 140,550 |
| Producer throughput (unthrottled) | about 95,000 messages/s |
| Produce-to-process latency at 600x replay | median 1.03 s, p95 1.93 s, max 2.39 s |
| Spark throughput per query (live) | 1.0k-1.6k rows/s average, 1.7k-3.6k peak |
| Backlog drain (all four queries) | 135,028 rows in about 40 s including Spark start-up |

Anomaly detection was evaluated on a held-out final session with 1% of bars replaced by synthetic
price shocks (3 to 12 standard deviations), volume surges (3x to 10x) or both.

| Detector | Precision | Recall | F1 | AUC-ROC | AUC-PR | Volume-only recall |
|---|---|---|---|---|---|---|
| MLlib KMeans | 0.39 | 0.69 | 0.50 | 0.968 | 0.630 | 0.12 |
| Return z-score | 0.58 | 0.64 | 0.61 | 0.862 | 0.620 | 0.00 |

At the fixed thresholds the z-score rule has the better F1. The held-out session contains a lasting
level change in one stock (TMUS gapped down about 9%): 116 of the session's 215 KMeans flags are
that symbol, because the static baseline keeps describing the old regime. Excluding TMUS the
held-out flag rate is 0.53%, in line with 0.49% in training. Ranking quality is where the
multivariate model is clearly ahead (AUC-ROC 0.968 against 0.862), and it is the only detector
that notices volume-only events.

## Report

`report/main.tex` is the IEEE-format case-study report (`IEEEtran`, two-column). Its figures are in
`report/figures/`. Compile it with `pdflatex main.tex` from inside `report/`, or upload the folder to
Overleaf and set `main.tex` as the main document.

## Limitations

- One week of one-minute bars is modest in volume. The architecture, not the data size, is what the
  project demonstrates, and the producer's speed multiplier is used to stress it.
- The replay is historical, so latency figures describe the pipeline and not a live exchange feed.
- The evaluation uses injected anomalies because real market anomalies are unlabelled.
- The anomaly baseline is static, so a lasting level change keeps raising flags until the model is retrained.
- Spark runs in local mode on a single machine; the same code runs on a cluster by changing the
  master and the Kafka and JDBC endpoints.
