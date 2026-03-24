from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    GITHUB_TOKEN: str
    GITHUB_REPO: str
    DATABASE_URL: str

    class Config:
        env_file = ".env"


settings = Settings()
