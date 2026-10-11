import logging

import psycopg2
from pyspark.sql.streaming import StreamingQueryListener

from nasdaq_stream.config import POSTGRES

LOGGER = logging.getLogger("metrics")

INSERT = """
    INSERT INTO pipeline_metrics (
        query_name, batch_id, input_rows, input_rows_per_sec,
        processed_rows_per_sec, batch_duration_ms
    ) VALUES (%s, %s, %s, %s, %s, %s)
"""


class PostgresMetricsListener(StreamingQueryListener):
    def __init__(self) -> None:
        super().__init__()
        self._connection = None

    def _cursor(self):
        if self._connection is None or self._connection.closed:
            self._connection = psycopg2.connect(
                host=POSTGRES.host,
                port=POSTGRES.port,
                dbname=POSTGRES.database,
                user=POSTGRES.user,
                password=POSTGRES.password,
            )
            self._connection.autocommit = True
        return self._connection.cursor()

    def onQueryStarted(self, event) -> None:
        LOGGER.info("query started: %s", event.name)

    def onQueryProgress(self, event) -> None:
        progress = event.progress
        if not progress.numInputRows:
            return
        try:
            with self._cursor() as cursor:
                cursor.execute(
                    INSERT,
                    (
                        progress.name,
                        progress.batchId,
                        progress.numInputRows,
                        progress.inputRowsPerSecond,
                        progress.processedRowsPerSecond,
                        progress.durationMs.get("triggerExecution", 0),
                    ),
                )
        except psycopg2.Error as error:
            LOGGER.warning("could not record metrics: %s", error)
            self._connection = None

    def onQueryIdle(self, event) -> None:
        return None

    def onQueryTerminated(self, event) -> None:
        LOGGER.info("query terminated: %s", event.id)
