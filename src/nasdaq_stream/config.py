import os
from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


@dataclass(frozen=True)
class KafkaConfig:
    bootstrap_servers: str = _env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic: str = _env("KAFKA_TOPIC", "ticks.raw")
    partitions: int = int(_env("KAFKA_PARTITIONS", "3"))


@dataclass(frozen=True)
class PostgresConfig:
    host: str = _env("POSTGRES_HOST", "localhost")
    port: int = int(_env("POSTGRES_PORT", "5433"))
    database: str = _env("POSTGRES_DB", "market")
    user: str = _env("POSTGRES_USER", "market")
    password: str = _env("POSTGRES_PASSWORD", "market")

    @property
    def jdbc_url(self) -> str:
        options = "reWriteBatchedInserts=true&sslmode=disable&connectTimeout=30&socketTimeout=120"
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}?{options}"


@dataclass(frozen=True)
class Paths:
    data_dir: Path = Path(_env("DATA_DIR", str(PROJECT_ROOT / "data")))
    model_dir: Path = Path(_env("MODEL_DIR", str(PROJECT_ROOT / "models")))
    checkpoint_dir: Path = Path(_env("CHECKPOINT_DIR", str(PROJECT_ROOT / "checkpoints")))

    @property
    def bars(self) -> Path:
        return self.data_dir / "bars.parquet"

    @property
    def anomaly_model(self) -> Path:
        return self.model_dir / "anomaly_kmeans"

    @property
    def evaluation_report(self) -> Path:
        return self.data_dir / "ml_evaluation.json"


KAFKA = KafkaConfig()
POSTGRES = PostgresConfig()
PATHS = Paths()
