from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class AppConfig:
    name: str
    env: str


@dataclass(frozen=True, slots=True)
class PostgresConfig:
    host: str
    port: int
    database: str
    user: str
    password: str
    pool_min_size: int
    pool_max_size: int
    connect_timeout_seconds: int


@dataclass(frozen=True, slots=True)
class KafkaConfig:
    bootstrap_servers: str
    events_v1_topic: str
    events_dlq_topic: str


@dataclass(frozen=True, slots=True)
class LoggingConfig:
    level: str
    output: str
    format: str
    file_path: str | None


@dataclass(frozen=True, slots=True)
class MetricsConfig:
    enabled: bool


@dataclass(frozen=True, slots=True)
class TracingConfig:
    enabled: bool


@dataclass(frozen=True, slots=True)
class OtelConfig:
    service_name: str
    logs_endpoint: str
    metrics_endpoint: str
    traces_endpoint: str
    metric_export_interval: int


@dataclass(frozen=True, slots=True)
class Config:
    app: AppConfig
    postgres: PostgresConfig
    kafka: KafkaConfig
    logging: LoggingConfig
    metrics: MetricsConfig
    tracing: TracingConfig
    otel: OtelConfig
