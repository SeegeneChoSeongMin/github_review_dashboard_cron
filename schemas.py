from datetime import date, datetime
from typing import Optional

from pydantic import BaseModel


# ── Developer ──────────────────────────────────────────────────────────────────

class DeveloperCreate(BaseModel):
    github_login: str
    name: Optional[str] = None
    team: Optional[str] = None
    department: Optional[str] = None
    is_active: bool = True


class DeveloperUpdate(BaseModel):
    name: Optional[str] = None
    team: Optional[str] = None
    department: Optional[str] = None
    is_active: Optional[bool] = None


class DeveloperResponse(BaseModel):
    id: int
    github_login: str
    name: Optional[str]
    team: Optional[str]
    department: Optional[str]
    is_active: bool
    created_at: datetime

    model_config = {"from_attributes": True}


# ── Weekly commit / line stats ─────────────────────────────────────────────────

class DeveloperWeeklyCommitsResponse(BaseModel):
    id: int
    repo: str
    github_login: str
    week_start: date
    additions: int
    deletions: int
    commits: int
    collected_at: datetime

    model_config = {"from_attributes": True}


# ── Merged PR line stats ──────────────────────────────────────────────────────

class DeveloperMergedPRLinesResponse(BaseModel):
    id: int
    repo: str
    pr_number: int
    github_login: str
    base_branch: str
    additions: int
    deletions: int
    merged_at: datetime
    collected_at: datetime

    model_config = {"from_attributes": True}


# ── PR / review activity snapshot ─────────────────────────────────────────────

class DeveloperPRActivityResponse(BaseModel):
    id: int
    repo: str
    github_login: str
    period_start: datetime
    period_end: datetime
    prs_opened: int
    prs_merged: int
    reviews_given: int
    review_comments_given: int
    collected_at: datetime

    model_config = {"from_attributes": True}
