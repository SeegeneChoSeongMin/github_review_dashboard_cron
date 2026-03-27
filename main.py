import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import List, Optional

from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException
from sqlalchemy.orm import Session

from config import settings
from database import get_db, init_db
from github_client import fetch_org_team_members
from models import Developer, DeveloperMergedPRLines, DeveloperPRActivity, DeveloperTeam, DeveloperWeeklyCommits
from scheduler import backfill_pr_data, collect_metrics, start_scheduler, stop_scheduler
from schemas import (
    DeveloperCreate,
    DeveloperMergedPRLinesResponse,
    DeveloperPRActivityResponse,
    DeveloperResponse,
    DeveloperUpdate,
    DeveloperWeeklyCommitsResponse,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    start_scheduler()
    yield
    stop_scheduler()


app = FastAPI(
    title="Developer Productivity Metrics",
    description="GitHub 저장소별 개발자 기여 지표(커밋/라인/PR/리뷰) 수집 및 조회",
    version="2.0.0",
    lifespan=lifespan,
)


# ── Developer 관리 (팀/사원 정보) ────────────────────────────────────────────────

@app.get("/developers", response_model=List[DeveloperResponse])
def list_developers(
    team: Optional[str] = None,
    db: Session = Depends(get_db),
):
    q = db.query(Developer)
    if team:
        q = q.filter(Developer.team == team)
    return q.order_by(Developer.github_login).all()


@app.post("/developers", response_model=DeveloperResponse, status_code=201)
def create_developer(body: DeveloperCreate, db: Session = Depends(get_db)):
    if db.query(Developer).filter(Developer.github_login == body.github_login).first():
        raise HTTPException(status_code=409, detail="github_login already exists")
    dev = Developer(**body.model_dump())
    db.add(dev)
    db.commit()
    db.refresh(dev)
    return dev


@app.put("/developers/{github_login}", response_model=DeveloperResponse)
def update_developer(
    github_login: str,
    body: DeveloperUpdate,
    db: Session = Depends(get_db),
):
    dev = db.query(Developer).filter(Developer.github_login == github_login).first()
    if not dev:
        raise HTTPException(status_code=404, detail="Developer not found")
    for field, value in body.model_dump(exclude_unset=True).items():
        setattr(dev, field, value)
    db.commit()
    db.refresh(dev)
    return dev


# ── 주간 커밋/라인 수 조회 ────────────────────────────────────────────────────────

@app.get("/commits", response_model=List[DeveloperWeeklyCommitsResponse])
def get_commit_stats(
    repo: Optional[str] = None,
    github_login: Optional[str] = None,
    limit: int = 200,
    db: Session = Depends(get_db),
):
    q = db.query(DeveloperWeeklyCommits)
    if repo:
        q = q.filter(DeveloperWeeklyCommits.repo == repo)
    if github_login:
        q = q.filter(DeveloperWeeklyCommits.github_login == github_login)
    return q.order_by(DeveloperWeeklyCommits.week_start.desc()).limit(limit).all()


# ── PR / 리뷰 활동 조회 ───────────────────────────────────────────────────────────

@app.get("/pr-activity", response_model=List[DeveloperPRActivityResponse])
def get_pr_activity(
    repo: Optional[str] = None,
    github_login: Optional[str] = None,
    limit: int = 200,
    db: Session = Depends(get_db),
):
    q = db.query(DeveloperPRActivity)
    if repo:
        q = q.filter(DeveloperPRActivity.repo == repo)
    if github_login:
        q = q.filter(DeveloperPRActivity.github_login == github_login)
    return q.order_by(DeveloperPRActivity.collected_at.desc()).limit(limit).all()


# ── Merged PR 파일 변경량 조회 ────────────────────────────────────────────────────

@app.get("/pr-lines", response_model=List[DeveloperMergedPRLinesResponse])
def get_merged_pr_lines(
    repo: Optional[str] = None,
    github_login: Optional[str] = None,
    base_branch: Optional[str] = None,
    limit: int = 500,
    db: Session = Depends(get_db),
):
    q = db.query(DeveloperMergedPRLines)
    if repo:
        q = q.filter(DeveloperMergedPRLines.repo == repo)
    if github_login:
        q = q.filter(DeveloperMergedPRLines.github_login == github_login)
    if base_branch:
        q = q.filter(DeveloperMergedPRLines.base_branch == base_branch)
    return q.order_by(DeveloperMergedPRLines.merged_at.desc()).limit(limit).all()


# ── 수동 수집 트리거 / 헬스 ──────────────────────────────────────────────────────

@app.post("/collect", status_code=202)
def trigger_collect():
    """스케줄을 기다리지 않고 즉시 수집을 실행합니다."""
    collect_metrics()
    return {"status": "collection triggered"}


@app.post("/backfill", status_code=202)
def trigger_backfill(
    since: str,
    background_tasks: BackgroundTasks,
):
    """
    과거 데이터 소급 수집 (백필).
    since: ISO 8601 날짜 문자열 (예: 2025-01-01 또는 2025-01-01T00:00:00Z)
    org 내 모든 repo에 대해 since 이후 merged PR 라인 변경량 + PR/리뷰 활동을 수집합니다.
    처리 시간이 길기 때문에 백그라운드로 실행되며, 진행 상황은 서버 로그에서 확인하세요.
    """
    try:
        since_dt = datetime.fromisoformat(since.rstrip("Z")).replace(tzinfo=timezone.utc)
    except ValueError:
        raise HTTPException(status_code=422, detail=f"Invalid date format: '{since}'. Use ISO 8601 (e.g. 2025-01-01)")

    background_tasks.add_task(backfill_pr_data, since_dt)
    return {"status": "backfill started", "since": since_dt.isoformat()}


@app.get("/health")
def health():
    return {"status": "ok"}


# ── GitHub Teams 동기화 ──────────────────────────────────────────────────────────

@app.post("/sync-teams", status_code=200)
def sync_teams(db: Session = Depends(get_db)):
    """
    GitHub org의 팀 멤버십을 가져와 developer_teams 테이블에 upsert합니다.
    - developers 테이블에 없는 login은 신규 생성
    - developer_teams는 (github_login, team) 쌍으로 다대다 관리
    토큰에 read:org 스코프가 필요합니다.
    """
    team_map = fetch_org_team_members(settings.GITHUB_ORG)

    dev_created = 0
    team_rows_upserted = 0

    for team_name, logins in team_map.items():
        for login in logins:
            # developers 테이블에 없으면 생성
            if not db.query(Developer).filter(Developer.github_login == login).first():
                db.add(Developer(github_login=login))
                db.flush()  # id 확보
                dev_created += 1

            # developer_teams upsert (이미 있으면 skip)
            exists = (
                db.query(DeveloperTeam)
                .filter(DeveloperTeam.github_login == login, DeveloperTeam.team == team_name)
                .first()
            )
            if not exists:
                db.add(DeveloperTeam(github_login=login, team=team_name))
                team_rows_upserted += 1

    db.commit()
    return {
        "synced_teams": list(team_map.keys()),
        "developers_created": dev_created,
        "team_memberships_added": team_rows_upserted,
    }
