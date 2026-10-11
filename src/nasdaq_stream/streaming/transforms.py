from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from nasdaq_stream.streaming.schema import BAR_SCHEMA, RAW_COLUMNS

WATERMARK = "2 minutes"
CANDLE_WINDOW = "5 minutes"
VOLATILITY_WINDOW = "15 minutes"
VOLATILITY_SLIDE = "1 minute"


def parse_bars(kafka_stream: DataFrame) -> DataFrame:
    parsed = kafka_stream.select(
        F.from_json(F.col("value").cast("string"), BAR_SCHEMA).alias("bar"),
        F.col("timestamp").alias("kafka_time"),
    )
    return (
        parsed.select("bar.*", "kafka_time")
        .withColumn("bar_time", F.to_timestamp("bar_time"))
        .withColumn("processed_time", F.current_timestamp())
        .select(*RAW_COLUMNS)
    )


def five_minute_candles(bars: DataFrame) -> DataFrame:
    typical_price = (F.col("high") + F.col("low") + F.col("close")) / 3
    candles = (
        bars.withWatermark("bar_time", WATERMARK)
        .groupBy(F.window("bar_time", CANDLE_WINDOW), "symbol")
        .agg(
            F.min_by("open", "bar_time").alias("open"),
            F.max("high").alias("high"),
            F.min("low").alias("low"),
            F.max_by("close", "bar_time").alias("close"),
            F.sum("volume").alias("volume"),
            F.try_divide(F.sum(typical_price * F.col("volume")), F.sum("volume")).alias("vwap"),
            F.count("*").alias("bar_count"),
        )
    )
    return candles.select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "symbol",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "vwap",
        (F.col("close") / F.col("open") - 1).alias("pct_change"),
        "bar_count",
    )


def rolling_volatility(bars: DataFrame) -> DataFrame:
    returns = bars.withColumn("bar_return", F.log(F.col("close") / F.col("open")))
    windows = (
        returns.withWatermark("bar_time", WATERMARK)
        .groupBy(F.window("bar_time", VOLATILITY_WINDOW, VOLATILITY_SLIDE), "symbol")
        .agg(
            F.stddev_samp("bar_return").alias("volatility"),
            F.avg("bar_return").alias("avg_return"),
            F.sum("volume").alias("volume"),
            F.count("*").alias("bar_count"),
        )
    )
    return windows.select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "symbol",
        "volatility",
        "avg_return",
        "volume",
        "bar_count",
    )
