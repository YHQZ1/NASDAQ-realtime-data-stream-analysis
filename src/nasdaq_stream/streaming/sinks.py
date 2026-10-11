from collections.abc import Callable

from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery

from nasdaq_stream.config import PATHS, POSTGRES

JDBC_BATCH_SIZE = 5000


def jdbc_writer(table: str) -> Callable[[DataFrame, int], None]:
    def write(batch: DataFrame, _batch_id: int) -> None:
        (
            batch.write.format("jdbc")
            .option("url", POSTGRES.jdbc_url)
            .option("dbtable", table)
            .option("user", POSTGRES.user)
            .option("password", POSTGRES.password)
            .option("driver", "org.postgresql.Driver")
            .option("batchsize", JDBC_BATCH_SIZE)
            .mode("append")
            .save()
        )

    return write


def start_query(stream: DataFrame, name: str, trigger_seconds: int, drain: bool) -> StreamingQuery:
    writer = (
        stream.writeStream.queryName(name)
        .outputMode("append")
        .foreachBatch(jdbc_writer(name))
        .option("checkpointLocation", str(PATHS.checkpoint_dir / name))
    )
    if drain:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=f"{trigger_seconds} seconds")
    return writer.start()
