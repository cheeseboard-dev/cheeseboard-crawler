from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    app_name: str = "CheeseBoard Crawler"
    debug: bool = False

    chzzk_base_url: str = "https://api.chzzk.naver.com/service/v1"
    request_timeout: float = 10.0
    max_concurrent_requests: int = 3
    retry_count: int = 3

    streamers_csv_path: str = "data/streamers.csv"
    output_dir: str = "output"

    # PostgreSQL
    database_url: str = "postgresql://cheeseboard:cheeseboard@localhost:5432/cheeseboard"


settings = Settings()
