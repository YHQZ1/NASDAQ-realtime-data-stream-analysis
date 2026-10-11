import json
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import reduce
from operator import add
from pathlib import Path

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from nasdaq_stream.ml.features import FEATURE_COLUMNS, add_zscores, compute_baseline

CLUSTERS = 4
Z_CLIP = 10.0
Z_THRESHOLD = 4.0
TRIM_QUANTILE = 0.99
THRESHOLD_QUANTILE = 0.995
SEED = 42

ANOMALY_COLUMNS = [
    "symbol",
    "bar_time",
    "close",
    "volume",
    "bar_return",
    "z_return",
    "z_range",
    "z_volume",
    "cluster",
    "kmeans_score",
    "kmeans_anomaly",
    "zscore_anomaly",
]


@dataclass(frozen=True)
class AnomalyModel:
    pipeline: PipelineModel
    baseline: DataFrame
    centers: list[list[float]]
    threshold: float
    z_threshold: float
    z_clip: float
    metadata: dict


def distance_to_nearest_center(features: str, centers: list[list[float]]) -> Column:
    values = vector_to_array(F.col(features))
    distances = []
    for center in centers:
        squares = [(values[i] - F.lit(c)) ** 2 for i, c in enumerate(center)]
        distances.append(F.sqrt(reduce(add, squares)))
    return F.least(*distances)


def _pipeline() -> Pipeline:
    assembler = VectorAssembler(inputCols=FEATURE_COLUMNS, outputCol="features")
    kmeans = KMeans(
        k=CLUSTERS, seed=SEED, featuresCol="features", predictionCol="cluster", maxIter=50
    )
    return Pipeline(stages=[assembler, kmeans])


def _centers(model: PipelineModel) -> list[list[float]]:
    return [center.tolist() for center in model.stages[-1].clusterCenters()]


def split_holdout(bars: DataFrame) -> tuple[DataFrame, DataFrame]:
    last_day = bars.agg(F.to_date(F.max("bar_time"))).first()[0]
    day = F.to_date("bar_time")
    return bars.filter(day < F.lit(last_day)), bars.filter(day == F.lit(last_day))


def train(bars: DataFrame) -> AnomalyModel:
    baseline = compute_baseline(bars).cache()
    features = add_zscores(bars, baseline, Z_CLIP).cache()

    first_pass = _pipeline().fit(features)
    first_centers = _centers(first_pass)
    distances = first_pass.transform(features).withColumn(
        "distance", distance_to_nearest_center("features", first_centers)
    )
    trim_at = distances.approxQuantile("distance", [TRIM_QUANTILE], 0.001)[0]
    trimmed = distances.filter(F.col("distance") <= trim_at).select(*features.columns)

    pipeline = _pipeline().fit(trimmed)
    centers = _centers(pipeline)
    final = pipeline.transform(features).withColumn(
        "distance", distance_to_nearest_center("features", centers)
    )
    threshold = final.approxQuantile("distance", [THRESHOLD_QUANTILE], 0.001)[0]

    metadata = {
        "clusters": CLUSTERS,
        "centers": centers,
        "feature_columns": FEATURE_COLUMNS,
        "threshold": threshold,
        "threshold_quantile": THRESHOLD_QUANTILE,
        "trim_quantile": TRIM_QUANTILE,
        "z_threshold": Z_THRESHOLD,
        "z_clip": Z_CLIP,
        "train_rows": features.count(),
        "trained_at": datetime.now(UTC).isoformat(),
    }
    return AnomalyModel(
        pipeline=pipeline,
        baseline=baseline,
        centers=centers,
        threshold=threshold,
        z_threshold=Z_THRESHOLD,
        z_clip=Z_CLIP,
        metadata=metadata,
    )


def score_bars(bars: DataFrame, model: AnomalyModel) -> DataFrame:
    features = add_zscores(bars, model.baseline, model.z_clip)
    return (
        model.pipeline.transform(features)
        .withColumn("kmeans_score", distance_to_nearest_center("features", model.centers))
        .withColumn("kmeans_anomaly", F.col("kmeans_score") > F.lit(model.threshold))
        .withColumn("zscore_anomaly", F.col("abs_z_return") > F.lit(model.z_threshold))
    )


def save_model(model: AnomalyModel, directory: Path) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    model.pipeline.write().overwrite().save(str(directory / "pipeline"))
    model.baseline.write.mode("overwrite").parquet(str(directory / "baseline"))
    (directory / "metadata.json").write_text(json.dumps(model.metadata, indent=2))


def load_model(spark: SparkSession, directory: Path) -> AnomalyModel:
    metadata = json.loads((directory / "metadata.json").read_text())
    return AnomalyModel(
        pipeline=PipelineModel.load(str(directory / "pipeline")),
        baseline=spark.read.parquet(str(directory / "baseline")).cache(),
        centers=metadata["centers"],
        threshold=metadata["threshold"],
        z_threshold=metadata["z_threshold"],
        z_clip=metadata["z_clip"],
        metadata=metadata,
    )
