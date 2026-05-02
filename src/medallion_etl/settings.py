from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    env: str = "dev"
    storage_root: str = "file://./data"
    log_level: str = "INFO"
    quarantine_threshold: float = Field(0.20, ge=0.0, le=1.0)


settings = Settings()
