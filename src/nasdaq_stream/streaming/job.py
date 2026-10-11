import argparse
import logging

from pyspark.sql import DataFrame, SparkSession

from nasdaq_stream.config import KAFKA, PATHS
from nasdaq_stream.ml.model import ANOMALY_COLUMNS, load_model, score_bars
from nasdaq_stream.spark_session import POSTGRES_PACKAGE, build_session, kafka_package
from nasdaq_stream.streaming.metrics import PostgresMetricsListener
from nasdaq_stream.streaming.sinks import start_query
from nasdaq_stream.streaming.transforms import (
    five_minute_candles,
    parse_bars,
    rolling_volatility,
)

LOGGER = logging.getLogger("job")


def read_kafka(spark: SparkSession, starting_offsets: str, max_offsets: int) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA.bootstrap_servers)
        .option("subscribe", KAFKA.topic)
        .option("startingOffsets", starting_offsets)
        .option("maxOffsetsPerTrigger", max_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the Structured Streaming pipeline")
    parser.add_argument("--starting-offsets", default="earliest")
    parser.add_argument("--max-offsets-per-trigger", type=int, default=20000)
    parser.add_argument("--trigger-seconds", type=int, default=2)
    parser.add_argument("--drain", action="store_true")
    parser.add_argument("--no-ml", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    logging.getLogger("py4j").setLevel(logging.WARNING)
    spark = build_session(
        "nasdaq-streaming", packages=[kafka_package(), POSTGRES_PACKAGE], driver_memory="3g"
    )
    listener = PostgresMetricsListener()
    spark.streams.addListener(listener)

    bars = parse_bars(read_kafka(spark, args.starting_offsets, args.max_offsets_per_trigger))
    streams = {
        "raw_bars": bars,
        "agg_5m": five_minute_candles(bars),
        "volatility": rolling_volatility(bars),
    }
    if not args.no_ml and PATHS.anomaly_model.exists():
        model = load_model(spark, PATHS.anomaly_model)
        streams["anomaly_scores"] = score_bars(bars, model).select(*ANOMALY_COLUMNS)
    elif not args.no_ml:
        LOGGER.warning("no trained model at %s; anomaly scoring disabled", PATHS.anomaly_model)

    queries = [
        start_query(stream, name, args.trigger_seconds, args.drain)
        for name, stream in streams.items()
    ]
    LOGGER.info("running queries: %s", ", ".join(query.name for query in queries))
    if args.drain:
        for query in queries:
            query.awaitTermination()
        spark.streams.removeListener(listener)
        spark.stop()
    else:
        spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
