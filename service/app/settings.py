from __future__ import annotations

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    DOCCLI_INTERNAL_API_KEY: str = ""
    DOCCLI_DATABASE_URL: str = "sqlite+aiosqlite:////data/doccli.db"

    # Backend owns storage credentials; doccli sends bytes to backend upload endpoint.
    BACKEND_INTERNAL_UPLOAD_URL: str = "http://backend:8000/api/v1/internal/media/upload"

    # Choreo
    CHOREO_SERVER_URL: str = "http://choreo:8080"

    # HybrIE (OpenAI-style /v1/*)
    HYBRIE_API_URL: str = "http://hybrie:8080"
    HYBRIE_API_KEY: str = ""
    DOCCLI_VISION_MODEL: str = "Qwen/Qwen2.5-VL-72B-Instruct"


settings = Settings()
