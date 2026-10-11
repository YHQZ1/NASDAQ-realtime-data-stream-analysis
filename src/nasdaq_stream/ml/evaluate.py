import argparse
import json
import logging
from pathlib import Path

from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from nasdaq_stream.config import PATHS
from nasdaq_stream.ml.features import KEY_COLUMNS, add_base_features
from nasdaq_stream.ml.model import AnomalyModel, load_model, score_bars, split_holdout
from nasdaq_stream.spark_session import build_session

LOGGER = logging.getLogger("evaluate")

INJECTION_RATE = 0.01
SHOCK_SIGMAS = (3.0, 12.0)
VOLUME_MULTIPLIERS = (3.0, 10.0)
DETECTORS = {
    "kmeans": ("kmeans_anomaly", "kmeans_score"),
    "zscore": ("zscore_anomaly", "abs_z_return"),
}
RANKING_METRICS = {"auc_roc": "areaUnderROC", "auc_pr": "areaUnderPR"}


def _uniform(low: float, high: float, seed: int):
    return F.lit(low) + F.lit(high - low) * F.rand(seed)


def inject_anomalies(holdout: DataFrame, model: AnomalyModel, seed: int) -> DataFrame:
    original_columns = holdout.columns
    spread = model.baseline.select(*KEY_COLUMNS, "ret_std")
    frame = (
        add_base_features(holdout)
        .join(F.broadcast(spread), KEY_COLUMNS, "inner")
        .withColumn("draw", F.rand(seed))
        .withColumn("kind", F.rand(seed + 1))
        .withColumn("sign", F.when(F.rand(seed + 2) < 0.5, 1.0).otherwise(-1.0))
        .withColumn("sigmas", _uniform(*SHOCK_SIGMAS, seed + 3))
        .withColumn("multiplier", _uniform(*VOLUME_MULTIPLIERS, seed + 4))
        .cache()
    )
    injected = F.col("draw") < INJECTION_RATE
    kind = F.col("kind")
    anomaly_type = (
        F.when(~injected, F.lit(None).cast("string"))
        .when(kind < 1 / 3, "price")
        .when(kind < 2 / 3, "volume")
        .otherwise("both")
    )
    shocked = F.col("anomaly_type").isin("price", "both")
    surged = F.col("anomaly_type").isin("volume", "both")
    shocked_close = F.col("close") * F.exp(F.col("sign") * F.col("sigmas") * F.col("ret_std"))
    return (
        frame.withColumn("anomaly_type", anomaly_type)
        .withColumn("close", F.when(shocked, shocked_close).otherwise(F.col("close")))
        .withColumn("high", F.greatest("high", "close"))
        .withColumn("low", F.least("low", "close"))
        .withColumn(
            "volume",
            F.when(surged, F.round(F.col("volume") * F.col("multiplier")).cast("long")).otherwise(
                F.col("volume")
            ),
        )
        .withColumn("injected", F.col("anomaly_type").isNotNull())
        .select(*original_columns, "anomaly_type", "injected")
    )


def ranking_metrics(scored: DataFrame, score: str) -> dict:
    labelled = scored.select(
        F.col(score).alias("score"), F.col("injected").cast("double").alias("label")
    )
    return {
        name: BinaryClassificationEvaluator(
            rawPredictionCol="score", labelCol="label", metricName=metric
        ).evaluate(labelled)
        for name, metric in RANKING_METRICS.items()
    }


def detector_metrics(scored: DataFrame, flag: str, score: str) -> dict:
    flagged = F.col(flag)
    injected = F.col("injected")
    row = scored.agg(
        F.sum((flagged & injected).cast("int")).alias("true_positive"),
        F.sum((flagged & ~injected).cast("int")).alias("false_positive"),
        F.sum((~flagged & injected).cast("int")).alias("false_negative"),
        F.sum((~injected).cast("int")).alias("clean_rows"),
    ).first()
    tp, fp, fn = row["true_positive"], row["false_positive"], row["false_negative"]
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    by_type = {
        record["anomaly_type"]: record["recall"]
        for record in scored.filter(injected)
        .groupBy("anomaly_type")
        .agg(F.avg(flagged.cast("double")).alias("recall"))
        .collect()
    }
    return {
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "false_positive_rate": fp / row["clean_rows"],
        **ranking_metrics(scored, score),
        "recall_by_type": by_type,
        "true_positive": tp,
        "false_positive": fp,
        "false_negative": fn,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Evaluate anomaly detectors with injected anomalies"
    )
    parser.add_argument("--input", type=Path, default=PATHS.bars)
    parser.add_argument("--model", type=Path, default=PATHS.anomaly_model)
    parser.add_argument("--output", type=Path, default=PATHS.evaluation_report)
    parser.add_argument("--seed", type=int, default=7)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    spark = build_session("nasdaq-evaluate")
    model = load_model(spark, args.model)
    _, holdout = split_holdout(spark.read.parquet(str(args.input)))

    labelled = inject_anomalies(holdout, model, args.seed)
    scored = score_bars(labelled, model).cache()
    report = {
        "holdout_rows": scored.count(),
        "injected_rows": scored.filter("injected").count(),
        "injection_rate": INJECTION_RATE,
        "shock_sigmas": SHOCK_SIGMAS,
        "volume_multipliers": VOLUME_MULTIPLIERS,
        "detectors": {
            name: detector_metrics(scored, *columns) for name, columns in DETECTORS.items()
        },
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2))
    LOGGER.info("evaluation report:\n%s", json.dumps(report, indent=2))
    spark.stop()


if __name__ == "__main__":
    main()
