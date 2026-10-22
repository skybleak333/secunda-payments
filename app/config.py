from functools import lru_cache

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+asyncpg://payments:payments@localhost:55437/payments"
    rabbitmq_url: str = "amqp://payments:payments@localhost:5678/"
    api_key: SecretStr = SecretStr("local-development-key")
    webhook_allowed_hosts: list[str] = ["webhook"]
    webhook_timeout: float = Field(default=5, gt=0)
    outbox_poll_interval: float = Field(default=0.25, gt=0)
    retry_base_delay: float = Field(default=1, gt=0)
    gateway_min_delay: float = Field(default=2, ge=0)
    gateway_max_delay: float = Field(default=5, ge=0)
    gateway_success_rate: float = Field(default=0.9, ge=0, le=1)


@lru_cache
def get_settings() -> Settings:
    return Settings()
