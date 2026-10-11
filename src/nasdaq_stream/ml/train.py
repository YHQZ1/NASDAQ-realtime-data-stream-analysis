import argparse
import logging
from pathlib import Path

from pyspark.sql import functions as F

from nasdaq_stream.config import PATHS
from nasdaq_stream.ml.model import save_model, score_bars, split_holdout, train
from nasdaq_stream.spark_session import build_session

LOGGER = logging.getLogger("train")


def flag_rates(scored) -> dict:
    row = scored.agg(
        F.count("*").alias("rows"),
        F.avg(F.col("kmeans_anomaly").cast("double")).alias("kmeans"),
        F.avg(F.col("zscore_anomaly").cast("double")).alias("zscore"),
    ).first()
    return {"rows": row["rows"], "kmeans": row["kmeans"], "zscore": row["zscore"]}


def main() -> None:
    parser = argparse.ArgumentParser(description="Train the MLlib anomaly detection model")
    parser.add_argument("--input", type=Path, default=PATHS.bars)
    parser.add_argument("--output", type=Path, default=PATHS.anomaly_model)
    parser.add_argument("--no-holdout", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    spark = build_session("nasdaq-train")
    bars = spark.read.parquet(str(args.input))
    if args.no_holdout:
        train_bars, holdout = bars, None
    else:
        train_bars, holdout = split_holdout(bars)

    model = train(train_bars)
    save_model(model, args.output)
    LOGGER.info("trained on %d bars; threshold %.4f", model.metadata["train_rows"], model.threshold)
    for index, center in enumerate(model.centers):
        LOGGER.info("centre %d: %s", index, [round(value, 3) for value in center])

    LOGGER.info("train flag rates: %s", flag_rates(score_bars(train_bars, model)))
    if holdout is not None:
        LOGGER.info("holdout flag rates: %s", flag_rates(score_bars(holdout, model)))
    LOGGER.info("saved model to %s", args.output)
    spark.stop()


if __name__ == "__main__":
    main()
