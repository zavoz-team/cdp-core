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
class Config:
    app: AppConfig
    postgres: PostgresConfig
