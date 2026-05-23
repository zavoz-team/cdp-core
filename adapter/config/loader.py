import os

import yaml
from dotenv import load_dotenv

from domain.config import (
    AppConfig,
    Config,
    KafkaConfig,
    LoggingConfig,
    MetricsConfig,
    OtelConfig,
    PostgresConfig,
    TracingConfig,
)


def load_config(path: str = 'config/app.yaml') -> Config:
    load_dotenv()

    with open(path) as f:
        raw = yaml.safe_load(f)

    app_raw = _require_section(raw, 'app')
    postgres_raw = _require_section(raw, 'postgres')
    kafka_raw = _require_section(raw, 'kafka')
    logging_raw = _require_section(raw, 'logging')
    metrics_raw = _require_section(raw, 'metrics')
    tracing_raw = _require_section(raw, 'tracing')
    otel_raw = _require_section(raw, 'otel')

    _apply_env_overrides(postgres_raw, kafka_raw, otel_raw)

    return Config(
        app=AppConfig(
            name=_require_str(app_raw, 'name', 'app.name'),
            env=_require_str(app_raw, 'env', 'app.env'),
        ),
        postgres=PostgresConfig(
            host=_require_str(postgres_raw, 'host', 'postgres.host'),
            port=int(_require_str(postgres_raw, 'port', 'postgres.port')),
            database=_require_str(postgres_raw, 'database', 'postgres.database'),
            user=_require_str(postgres_raw, 'user', 'postgres.user'),
            password=_require_str(postgres_raw, 'password', 'postgres.password'),
            pool_min_size=int(
                _require_str(postgres_raw, 'pool_min_size', 'postgres.pool_min_size')
            ),
            pool_max_size=int(
                _require_str(postgres_raw, 'pool_max_size', 'postgres.pool_max_size')
            ),
            connect_timeout_seconds=int(
                _require_str(
                    postgres_raw,
                    'connect_timeout_seconds',
                    'postgres.connect_timeout_seconds',
                )
            ),
        ),
        kafka=KafkaConfig(
            bootstrap_servers=_require_str(
                kafka_raw, 'bootstrap_servers', 'kafka.bootstrap_servers'
            ),
            events_v1_topic=_require_str(
                kafka_raw, 'events_v1_topic', 'kafka.events_v1_topic'
            ),
            events_dlq_topic=_require_str(
                kafka_raw, 'events_dlq_topic', 'kafka.events_dlq_topic'
            ),
        ),
        logging=LoggingConfig(
            level=_require_str(logging_raw, 'level', 'logging.level'),
            output=_require_str(logging_raw, 'output', 'logging.output'),
            format=_require_str(logging_raw, 'format', 'logging.format'),
            file_path=logging_raw.get('file_path'),
        ),
        metrics=MetricsConfig(
            enabled=bool(metrics_raw.get('enabled', False)),
        ),
        tracing=TracingConfig(
            enabled=bool(tracing_raw.get('enabled', False)),
        ),
        otel=OtelConfig(
            service_name=_require_str(otel_raw, 'service_name', 'otel.service_name'),
            logs_endpoint=_require_str(otel_raw, 'logs_endpoint', 'otel.logs_endpoint'),
            metrics_endpoint=_require_str(
                otel_raw, 'metrics_endpoint', 'otel.metrics_endpoint'
            ),
            traces_endpoint=_require_str(
                otel_raw, 'traces_endpoint', 'otel.traces_endpoint'
            ),
            metric_export_interval=int(
                _require_str(
                    otel_raw, 'metric_export_interval', 'otel.metric_export_interval'
                )
            ),
        ),
    )


def _apply_env_overrides(postgres: dict, kafka: dict, otel: dict) -> None:
    _override_str(postgres, 'host', 'CDP_CORE_POSTGRES_HOST')
    _override_str(postgres, 'port', 'CDP_CORE_POSTGRES_PORT')
    _override_str(postgres, 'database', 'CDP_CORE_POSTGRES_DATABASE')
    _override_str(postgres, 'user', 'CDP_CORE_POSTGRES_USER')
    _override_str(postgres, 'password', 'CDP_CORE_POSTGRES_PASSWORD')

    _override_str(kafka, 'bootstrap_servers', 'CDP_CORE_KAFKA_BOOTSTRAP_SERVERS')
    _override_str(kafka, 'events_v1_topic', 'CDP_CORE_KAFKA_EVENTS_V1_TOPIC')
    _override_str(kafka, 'events_dlq_topic', 'CDP_CORE_KAFKA_EVENTS_DLQ_TOPIC')

    _override_str(otel, 'service_name', 'CDP_CORE_OTEL_SERVICE_NAME')
    _override_str(otel, 'logs_endpoint', 'CDP_CORE_OTEL_LOGS_ENDPOINT')
    _override_str(otel, 'metrics_endpoint', 'CDP_CORE_OTEL_METRICS_ENDPOINT')
    _override_str(otel, 'traces_endpoint', 'CDP_CORE_OTEL_TRACES_ENDPOINT')


def _override_str(target: dict, key: str, env_var: str) -> None:
    value = os.environ.get(env_var)
    if value is not None:
        target[key] = value


def _require_section(raw: dict, key: str) -> dict:
    section = raw.get(key)
    if not isinstance(section, dict):
        raise ValueError(f'missing config section: {key}')
    return section


def _require_str(section: dict, key: str, full_key: str) -> str:
    value = section.get(key)
    if value is None:
        raise ValueError(f'missing required config field: {full_key}')
    return str(value)
