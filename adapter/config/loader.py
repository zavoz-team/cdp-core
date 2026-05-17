import os

import yaml

from domain.config import AppConfig, Config, PostgresConfig


def load_config(path: str = 'config/app.yaml') -> Config:
    with open(path) as f:
        raw = yaml.safe_load(f)

    app_raw = _require_section(raw, 'app')
    postgres_raw = _require_section(raw, 'postgres')

    _apply_env_overrides(postgres_raw)

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
            pool_min_size=int(_require_str(postgres_raw, 'pool_min_size', 'postgres.pool_min_size')),
            pool_max_size=int(_require_str(postgres_raw, 'pool_max_size', 'postgres.pool_max_size')),
            connect_timeout_seconds=int(
                _require_str(postgres_raw, 'connect_timeout_seconds', 'postgres.connect_timeout_seconds')
            ),
        ),
    )


def _apply_env_overrides(postgres: dict) -> None:
    _override_str(postgres, 'host', 'CDP_CORE_POSTGRES_HOST')
    _override_str(postgres, 'port', 'CDP_CORE_POSTGRES_PORT')
    _override_str(postgres, 'database', 'CDP_CORE_POSTGRES_DATABASE')
    _override_str(postgres, 'user', 'CDP_CORE_POSTGRES_USER')
    _override_str(postgres, 'password', 'CDP_CORE_POSTGRES_PASSWORD')


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