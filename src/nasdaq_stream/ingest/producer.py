import argparse
import json
import logging
import time
from pathlib import Path

import pandas as pd
from confluent_kafka import KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from nasdaq_stream.config import KAFKA, PATHS

LOGGER = logging.getLogger("producer")

PROGRESS_EVERY = 25


def ensure_topic(bootstrap_servers: str, topic: str, partitions: int) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    if topic in admin.list_topics(timeout=15).topics:
        return
    futures = admin.create_topics(
        [NewTopic(topic, num_partitions=partitions, replication_factor=1)]
    )
    for future in futures.values():
        future.result()
    LOGGER.info("created topic %s with %d partitions", topic, partitions)


def rebase_offset(bars: pd.DataFrame, now: pd.Timestamp) -> pd.Timedelta:
    last_day = bars["bar_time"].max().normalize()
    shift_days = max((now.normalize() - last_day).days - 1, 0)
    return pd.Timedelta(days=shift_days)


def encode(row: tuple, offset: pd.Timedelta) -> tuple[bytes, bytes]:
    symbol, bar_time, open_, high, low, close, volume = row
    payload = {
        "symbol": symbol,
        "bar_time": (bar_time + offset).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "volume": int(volume),
    }
    return symbol.encode(), json.dumps(payload, separators=(",", ":")).encode()


class DeliveryTracker:
    def __init__(self) -> None:
        self.delivered = 0
        self.failed = 0

    def __call__(self, err, _msg) -> None:
        if err is None:
            self.delivered += 1
        else:
            self.failed += 1
            LOGGER.error("delivery failed: %s", err)


def send(producer: Producer, topic: str, key: bytes, value: bytes, tracker: DeliveryTracker):
    while True:
        try:
            producer.produce(topic, key=key, value=value, on_delivery=tracker)
            return
        except BufferError:
            producer.poll(0.2)


def replay(bars: pd.DataFrame, speed: float, max_minutes: int | None, offset: pd.Timedelta):
    producer = Producer(
        {
            "bootstrap.servers": KAFKA.bootstrap_servers,
            "linger.ms": 20,
            "compression.type": "lz4",
            "acks": "all",
            "enable.idempotence": True,
        }
    )
    tracker = DeliveryTracker()
    interval = 60.0 / speed if speed > 0 else 0.0
    started = time.monotonic()
    sent = 0

    for minute_index, (_, group) in enumerate(bars.groupby("bar_time", sort=True), start=1):
        if max_minutes is not None and minute_index > max_minutes:
            break
        tick_started = time.monotonic()
        for row in group.itertuples(index=False, name=None):
            key, value = encode(row, offset)
            send(producer, KAFKA.topic, key, value, tracker)
            sent += 1
        producer.poll(0)
        if minute_index % PROGRESS_EVERY == 0:
            elapsed = time.monotonic() - started
            LOGGER.info(
                "minute %d: sent %d messages (%.0f msg/s)", minute_index, sent, sent / elapsed
            )
        remaining = interval - (time.monotonic() - tick_started)
        if remaining > 0:
            time.sleep(remaining)

    producer.flush(30)
    elapsed = time.monotonic() - started
    LOGGER.info(
        "finished: %d sent, %d delivered, %d failed in %.1fs",
        sent,
        tracker.delivered,
        tracker.failed,
        elapsed,
    )
    if tracker.failed:
        raise KafkaException(f"{tracker.failed} messages failed delivery")


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay 1-minute bars into Kafka")
    parser.add_argument("--input", type=Path, default=PATHS.bars)
    parser.add_argument(
        "--speed",
        type=float,
        default=60.0,
        help="replay speed multiplier; 60 sends one market minute per second, 0 is unthrottled",
    )
    parser.add_argument("--max-minutes", type=int, default=None)
    parser.add_argument("--no-rebase", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    bars = pd.read_parquet(args.input).sort_values(["bar_time", "symbol"], ignore_index=True)
    offset = pd.Timedelta(0) if args.no_rebase else rebase_offset(bars, pd.Timestamp.now(tz="UTC"))
    LOGGER.info(
        "loaded %d bars across %d symbols; timestamps shifted by %s",
        len(bars),
        bars["symbol"].nunique(),
        offset,
    )
    ensure_topic(KAFKA.bootstrap_servers, KAFKA.topic, KAFKA.partitions)
    replay(bars, args.speed, args.max_minutes, offset)


if __name__ == "__main__":
    main()
