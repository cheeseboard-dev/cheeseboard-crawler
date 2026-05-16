from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    app_name: str = "CheeseBoard Crawler"
    port: int = 8000
    debug: bool = False
    api_key_hash: str = ""
    discord_webhook_url: str = ""
    log_format: str = "json"

    chzzk_base_url: str = "https://api.chzzk.naver.com/service/v1"
    request_timeout: float = 10.0
    max_concurrent_requests: int = 3
    retry_count: int = 3

    default_video_pages: int = 10
    default_clip_pages: int = 5
    max_live_pages: int = 200

    streamers_csv_path: str = "data/streamers.csv"
    output_dir: str = "output"

    # PostgreSQL
    database_url: str = "postgresql://cheeseboard:cheeseboard@localhost:5432/cheeseboard"


settings = Settings()
