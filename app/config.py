from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+asyncpg://payments:payments@localhost:5432/payments"
    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"
    api_key: str = "secret-api-key"

    # Consumer / retry
    max_attempts: int = 3            # всего попыток обработки сообщения
    retry_base_delay: int = 5        # секунды; задержка перед попыткой n+1 = base * 2**(n-1)
    webhook_timeout: float = 10.0

    # Эмуляция платёжного шлюза
    gateway_min_delay: float = 2.0
    gateway_max_delay: float = 5.0
    gateway_success_rate: float = 0.9

    # Outbox relay
    outbox_poll_interval: float = 1.0
    outbox_batch_size: int = 50


settings = Settings()
