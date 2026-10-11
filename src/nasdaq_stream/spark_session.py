from collections.abc import Sequence

import pyspark
from pyspark.sql import SparkSession

POSTGRES_PACKAGE = "org.postgresql:postgresql:42.7.4"


def kafka_package() -> str:
    major = int(pyspark.__version__.split(".")[0])
    scala = "2.13" if major >= 4 else "2.12"
    return f"org.apache.spark:spark-sql-kafka-0-10_{scala}:{pyspark.__version__}"


def build_session(
    app_name: str,
    packages: Sequence[str] = (),
    driver_memory: str = "3g",
    shuffle_partitions: int = 6,
) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*,4]")
        .config("spark.driver.memory", driver_memory)
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.hadoop.fs.defaultFS", "file:///")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.ui.showConsoleProgress", "false")
    )
    if packages:
        builder = builder.config("spark.jars.packages", ",".join(packages))
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("WARN")
    return session
