from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str
    # Simulated external API call duration
    processing_delay_seconds: float = 30.0
    # How often the worker checks the DB for pending jobs when idle
    worker_poll_interval_seconds: float = 2.0
    # A claimed job is re-claimable after this lease expires (crash recovery).
    # Must be comfortably longer than processing_delay_seconds.
    job_lease_seconds: int = 120
    # Max transactions processed concurrently per instance
    worker_concurrency: int = 100
    # Supabase's session pooler caps clients at 15 on the free tier. Each instance uses
    # 4 + 3 = 7, leaving headroom for a second instance during Render's zero-downtime deploys.
    # Webhook ingestion gets its own pool so status reads and the worker can never delay a 202.
    db_pool_size_ingest: int = 4
    db_pool_size_general: int = 3
    # While idle, ingest connections are touched this often to keep them open and prepared
    ingest_keepalive_seconds: float = 60.0


settings = Settings()
