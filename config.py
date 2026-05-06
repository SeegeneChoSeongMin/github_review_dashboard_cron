from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    ALLOWED_GITHUB_TOKEN: str
    # 수집 대상 GitHub Organization
    GITHUB_ORG: str = "SG-STAgora"
    DATABASE_URL: str
    # 수집 대상 레포 목록 (콤마 구분). 비어있으면 org 전체 레포를 fetch.
    GITHUB_REPOS: str = ""

    class Config:
        env_file = ".env"


settings = Settings()
