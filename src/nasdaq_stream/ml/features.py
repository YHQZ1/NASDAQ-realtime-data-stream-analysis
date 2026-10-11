from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

BUCKET_MINUTES = 30
STD_FLOOR = 1e-6
KEY_COLUMNS = ["symbol", "bucket"]
BASE_METRICS = {"bar_return": "ret", "bar_range": "range", "log_volume": "vol"}
FEATURE_COLUMNS = ["abs_z_return", "z_range", "z_volume"]


def add_base_features(bars: DataFrame) -> DataFrame:
    minute_of_day = F.hour("bar_time") * 60 + F.minute("bar_time")
    return (
        bars.withColumn("bar_return", F.log(F.col("close") / F.col("open")))
        .withColumn("bar_range", (F.col("high") - F.col("low")) / F.col("open"))
        .withColumn("log_volume", F.log1p(F.col("volume").cast("double")))
        .withColumn("bucket", F.floor(minute_of_day / BUCKET_MINUTES).cast("int"))
    )


def compute_baseline(bars: DataFrame) -> DataFrame:
    aggregations = []
    for column, prefix in BASE_METRICS.items():
        spread = F.coalesce(F.stddev_samp(column), F.lit(0.0))
        aggregations.append(F.avg(column).alias(f"{prefix}_mean"))
        aggregations.append(F.greatest(spread, F.lit(STD_FLOOR)).alias(f"{prefix}_std"))
    return add_base_features(bars).groupBy(*KEY_COLUMNS).agg(*aggregations)


def _standardise(column: str, prefix: str, clip: float) -> Column:
    z = (F.col(column) - F.col(f"{prefix}_mean")) / F.col(f"{prefix}_std")
    return F.greatest(F.least(z, F.lit(clip)), F.lit(-clip))


def add_zscores(bars: DataFrame, baseline: DataFrame, clip: float) -> DataFrame:
    joined = add_base_features(bars).join(F.broadcast(baseline), KEY_COLUMNS, "inner")
    return (
        joined.withColumn("z_return", _standardise("bar_return", "ret", clip))
        .withColumn("z_range", _standardise("bar_range", "range", clip))
        .withColumn("z_volume", _standardise("log_volume", "vol", clip))
        .withColumn("abs_z_return", F.abs("z_return"))
    )
