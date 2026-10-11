import argparse
import logging
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

import pandas as pd
from confluent_kafka.admin import AdminClient

from nasdaq_stream.config import KAFKA, PATHS, PROJECT_ROOT
from nasdaq_stream.ingest.producer import ensure_topic

LOGGER = logging.getLogger("demo")

TABLES = ("raw_bars", "agg_5m", "volatility", "anomaly_scores", "pipeline_metrics")
GRAFANA_URL = (
    "http://localhost:3000/d/nasdaq-realtime/nasdaq-real-time-market-analytics"
    "?orgId=1&from=now-10d&to=now&refresh=5s"
)
READY_MARKER = "running queries"
READY_TIMEOUT_SECONDS = 240
FLUSH_SECONDS = 20
VERIFY_TIMEOUT_SECONDS = 120


def compose_exec(service: str, *command: str) -> str:
    result = subprocess.run(
        ["docker", "compose", "exec", "-T", service, *command],
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout


def require_prerequisites() -> None:
    missing = []
    if not PATHS.bars.exists():
        missing.append("cached bars (run: make download)")
    if not (PATHS.anomaly_model / "metadata.json").exists():
        missing.append("trained model (run: make train)")
    running = subprocess.run(
        ["docker", "compose", "ps", "--status", "running", "--services"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
    ).stdout.split()
    for service in ("kafka", "postgres", "grafana"):
        if service not in running:
            missing.append(f"container {service} (run: make up)")
    if missing:
        raise SystemExit("missing prerequisites:\n  - " + "\n  - ".join(missing))


def reset_state() -> None:
    shutil.rmtree(PATHS.checkpoint_dir, ignore_errors=True)
    compose_exec(
        "postgres",
        "psql",
        "-U",
        "market",
        "-d",
        "market",
        "-c",
        f"TRUNCATE {', '.join(TABLES)}",
    )
    admin = AdminClient({"bootstrap.servers": KAFKA.bootstrap_servers})
    if KAFKA.topic in admin.list_topics(timeout=10).topics:
        for future in admin.delete_topics([KAFKA.topic]).values():
            future.result()
        deadline = time.monotonic() + 30
        while KAFKA.topic in admin.list_topics(timeout=10).topics:
            if time.monotonic() > deadline:
                raise SystemExit("topic deletion did not complete")
            time.sleep(1)
    ensure_topic(KAFKA.bootstrap_servers, KAFKA.topic, KAFKA.partitions)


def start_stream(log_path: Path) -> subprocess.Popen:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_file = log_path.open("w")
    process = subprocess.Popen(
        [sys.executable, "-m", "nasdaq_stream.streaming.job"],
        cwd=PROJECT_ROOT,
        stdout=log_file,
        stderr=subprocess.STDOUT,
    )
    deadline = time.monotonic() + READY_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise SystemExit(f"streaming job exited early; see {log_path}")
        if READY_MARKER in log_path.read_text(errors="replace"):
            return process
        time.sleep(1)
    process.terminate()
    raise SystemExit(f"streaming job was not ready within {READY_TIMEOUT_SECONDS}s")


def stop_stream(process: subprocess.Popen) -> None:
    process.send_signal(signal.SIGINT)
    try:
        process.wait(timeout=30)
    except subprocess.TimeoutExpired:
        process.terminate()
        process.wait(timeout=30)


def run_replay(speed: float, max_minutes: int | None) -> None:
    command = [sys.executable, "-m", "nasdaq_stream.ingest.producer", "--speed", str(speed)]
    if max_minutes is not None:
        command += ["--max-minutes", str(max_minutes)]
    subprocess.run(command, cwd=PROJECT_ROOT, check=True)


def expected_rows(max_minutes: int | None) -> int:
    times = pd.read_parquet(PATHS.bars, columns=["bar_time"])["bar_time"]
    if max_minutes is None:
        return len(times)
    first_minutes = times.drop_duplicates().sort_values().iloc[:max_minutes]
    return int(times.isin(first_minutes).sum())


def table_counts() -> dict[str, int]:
    query = " UNION ALL ".join(f"SELECT '{table}', count(*) FROM {table}" for table in TABLES)
    output = compose_exec(
        "postgres", "psql", "-U", "market", "-d", "market", "-t", "-A", "-F", ",", "-c", query
    )
    counts = {}
    for line in output.strip().splitlines():
        name, value = line.split(",")
        counts[name] = int(value)
    return counts


def verify(stream: subprocess.Popen, expected: int) -> bool:
    deadline = time.monotonic() + VERIFY_TIMEOUT_SECONDS
    counts = table_counts()
    while counts["raw_bars"] < expected and time.monotonic() < deadline:
        if stream.poll() is not None:
            break
        time.sleep(3)
        counts = table_counts()
    summary = ", ".join(f"{name}={value:,}" for name, value in counts.items())
    ok = counts["raw_bars"] == expected and counts["anomaly_scores"] == expected
    if ok:
        LOGGER.info("PASS: %d bars processed end to end (%s)", expected, summary)
    else:
        LOGGER.error(
            "FAIL: expected %d bars, found raw_bars=%d anomaly_scores=%d (%s)",
            expected,
            counts["raw_bars"],
            counts["anomaly_scores"],
            summary,
        )
        if stream.poll() is not None:
            LOGGER.error("the streaming job exited with code %s", stream.returncode)
    return ok


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the full pipeline end to end as a live demo")
    parser.add_argument("--speed", type=float, default=600.0)
    parser.add_argument("--max-minutes", type=int, default=None)
    parser.add_argument("--no-reset", action="store_true")
    parser.add_argument("--keep-running", action="store_true")
    parser.add_argument("--log", type=Path, default=PROJECT_ROOT / "demo" / "stream.log")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    require_prerequisites()
    if not args.no_reset:
        LOGGER.info("resetting checkpoints, tables and topic")
        reset_state()
    else:
        ensure_topic(KAFKA.bootstrap_servers, KAFKA.topic, KAFKA.partitions)

    LOGGER.info("starting Spark Structured Streaming job (log: %s)", args.log)
    stream = start_stream(args.log)
    LOGGER.info("pipeline ready. dashboard: %s", GRAFANA_URL)
    LOGGER.info("spark ui: http://localhost:4040   kafka ui: http://localhost:8080")

    passed = False
    try:
        run_replay(args.speed, args.max_minutes)
        LOGGER.info("replay finished; waiting for the last micro-batches")
        time.sleep(FLUSH_SECONDS)
        passed = verify(stream, expected_rows(args.max_minutes))
        if args.keep_running and passed:
            LOGGER.info("leaving the job running; press Ctrl-C to stop")
            stream.wait()
    except KeyboardInterrupt:
        LOGGER.info("interrupted")
        passed = True
    finally:
        if stream.poll() is None:
            stop_stream(stream)
    if not passed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
