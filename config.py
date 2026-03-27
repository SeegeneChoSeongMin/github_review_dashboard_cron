from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    ALLOWED_GITHUB_TOKEN: str
    # 수집 대상 GitHub Organization
    GITHUB_ORG: str = "SG-STAgora"
    DATABASE_URL: str

    class Config:
        env_file = ".env"


settings = Settings()
