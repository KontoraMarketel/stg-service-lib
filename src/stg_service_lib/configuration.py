from dataclasses import dataclass


@dataclass
class KafkaConfig:
    bootstrap_servers: str
    consumer_topic: str
    producer_topic: str
    group_id: str
    auto_offset_reset: str = "earliest"
    enable_auto_commit: bool = True


@dataclass
class MinioConfig:
    endpoint: str
    access_key: str
    secret_key: str
    bucket: str


@dataclass
class ClickHouseConfig:
    host: str
    port: int
    username: str
    password: str
    database: str


@dataclass
class Config:
    kafka: KafkaConfig
    minio: MinioConfig
    clickhouse: ClickHouseConfig
