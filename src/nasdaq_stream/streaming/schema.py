from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

BAR_SCHEMA = StructType(
    [
        StructField("symbol", StringType(), False),
        StructField("bar_time", StringType(), False),
        StructField("open", DoubleType(), False),
        StructField("high", DoubleType(), False),
        StructField("low", DoubleType(), False),
        StructField("close", DoubleType(), False),
        StructField("volume", LongType(), False),
    ]
)

RAW_COLUMNS = [
    "symbol",
    "bar_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "kafka_time",
    "processed_time",
]
