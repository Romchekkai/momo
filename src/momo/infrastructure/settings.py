from pydantic import Field, PostgresDsn
from pydantic_settings import BaseSettings, SettingsConfigDict


class DatabaseSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: PostgresDsn = PostgresDsn(
        "postgresql+psycopg://postgres:postgres@localhost:5432/momo_db"
    )
    echo: bool = False
    pool_size: int = 10
    max_overflow: int = 20

    @property
    def url(self) -> str:
        return str(self.database_url)


class KafkaSettings(BaseSettings):
    bootstrap_servers: str | list[str] = Field(default="localhost:9092")
    client_id: str = Field(default="momo-service")

    model_config = SettingsConfigDict(
        env_prefix="KAFKA_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )
