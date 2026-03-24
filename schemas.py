from datetime import datetime

from pydantic import BaseModel, Field


class RepoMetricsBase(BaseModel):
    repo: str
    stars: int = Field(..., ge=0)
    forks: int = Field(..., ge=0)
    open_issues: int = Field(..., ge=0)
    watchers: int = Field(..., ge=0)


class RepoMetricsCreate(RepoMetricsBase):
    pass


class RepoMetricsResponse(RepoMetricsBase):
    id: int
    collected_at: datetime

    model_config = {"from_attributes": True}


class GitHubRepoData(BaseModel):
    stargazers_count: int
    forks_count: int
    open_issues_count: int
    watchers_count: int
