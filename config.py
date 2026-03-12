from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    # HTTP Server
    host: str = "0.0.0.0"
    port: int = 8000

    # Database
    db_host: str = "localhost"
    db_port: int = 3306
    db_user: str = "root"
    db_password: str = "password"
    db_name: str = "notification_db"

    # Kafka
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "notification_requests"
    kafka_dlq_topic: str = "notification_dlq"
    kafka_consumer_group: str = "notification_consumer_group"

    # Consumer
    consumer_workers: int = 5

    # Retry
    max_retry_count: int = 5
    retry_scan_interval: int = 60  # 每 60 秒扫描一次重试任务

    class Config:
        env_file = ".env"

    @property
    def database_url(self) -> str:
        return f"mysql+pymysql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"


settings = Settings()
