import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import List

from fastapi import Depends, FastAPI, HTTPException
from sqlalchemy.orm import Session

from config import settings
from database import get_db, init_db
from github_client import fetch_repo_metrics
from models import RepoMetrics
from scheduler import start_scheduler, stop_scheduler
from schemas import RepoMetricsResponse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    start_scheduler()
    yield
    stop_scheduler()


app = FastAPI(
    title="GitHub Metrics Collector",
    description="Collects GitHub repository metrics and stores them in PostgreSQL",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/metrics", response_model=List[RepoMetricsResponse])
def get_metrics(
    limit: int = 100,
    db: Session = Depends(get_db),
):
    return (
        db.query(RepoMetrics)
        .filter(RepoMetrics.repo == settings.GITHUB_REPO)
        .order_by(RepoMetrics.collected_at.desc())
        .limit(limit)
        .all()
    )


@app.post("/metrics/collect", response_model=RepoMetricsResponse)
def trigger_collect(db: Session = Depends(get_db)):
    try:
        data = fetch_repo_metrics()
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"GitHub API error: {exc}") from exc

    record = RepoMetrics(
        repo=settings.GITHUB_REPO,
        stars=data.stargazers_count,
        forks=data.forks_count,
        open_issues=data.open_issues_count,
        watchers=data.watchers_count,
        collected_at=datetime.now(timezone.utc),
    )
    db.add(record)
    db.commit()
    db.refresh(record)
    return record


@app.get("/health")
def health():
    return {"status": "ok"}
