from datetime import date, datetime, timezone

from sqlalchemy import BigInteger, Boolean, Date, DateTime, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from database import Base


class Developer(Base):
    """사원/팀 정보 (GitHub cron과 별개로 수동 관리)."""

    __tablename__ = "developers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    github_login: Mapped[str] = mapped_column(
        String, unique=True, nullable=False, index=True
    )
    name: Mapped[str | None] = mapped_column(String, nullable=True)
    team: Mapped[str | None] = mapped_column(String, nullable=True)
    department: Mapped[str | None] = mapped_column(String, nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(timezone.utc)
    )


class DeveloperWeeklyCommits(Base):
    """GitHub stats/contributors API에서 수집한 주간 커밋/라인 변경 수."""

    __tablename__ = "developer_weekly_commits"
    __table_args__ = (
        UniqueConstraint(
            "repo", "github_login", "week_start", name="uq_weekly_commits"
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    repo: Mapped[str] = mapped_column(String, nullable=False, index=True)
    github_login: Mapped[str] = mapped_column(String, nullable=False, index=True)
    week_start: Mapped[date] = mapped_column(Date, nullable=False)
    additions: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    deletions: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    commits: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    collected_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(timezone.utc)
    )


class DeveloperMergedPRLines(Base):
    """
    merged PR 단위 파일 변경량.
    contrib stats의 'default branch 머지분만 집계' 제약을 보완하기 위해
    PR별 /pulls/{number}/files 를 직접 수집해 저장한다.
    PR number를 PK 대신 unique key로 두어 재수집 시 중복 없이 upsert 가능.
    """

    __tablename__ = "developer_merged_pr_lines"
    __table_args__ = (
        UniqueConstraint("repo", "pr_number", name="uq_merged_pr_lines"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    repo: Mapped[str] = mapped_column(String, nullable=False, index=True)
    pr_number: Mapped[int] = mapped_column(Integer, nullable=False)
    github_login: Mapped[str] = mapped_column(String, nullable=False, index=True)
    base_branch: Mapped[str] = mapped_column(String, nullable=False)
    additions: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    deletions: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    merged_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    collected_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(timezone.utc)
    )


class DeveloperReviewEvent(Base):
    """
    리뷰 1건당 1행. submitted_at 실제 날짜 기준으로 저장.
    review_type: 'review' (PR 승인/코멘트/변경요청) | 'review_comment' (인라인 코멘트)
    """

    __tablename__ = "developer_review_events"
    __table_args__ = (
        UniqueConstraint("repo", "review_id", "review_type", name="uq_review_event"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    repo: Mapped[str] = mapped_column(String, nullable=False, index=True)
    review_id: Mapped[int] = mapped_column(BigInteger, nullable=False)  # GitHub review/comment id
    review_type: Mapped[str] = mapped_column(String, nullable=False)  # 'review' | 'review_comment'
    pr_number: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    github_login: Mapped[str] = mapped_column(String, nullable=False, index=True)
    submitted_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    collected_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(timezone.utc)
    )


class DeveloperPREvent(Base):
    """
    PR 1건당 1행. opened/merged/closed 이벤트를 실제 발생 날짜로 저장.
    날짜 필터 기반 팀별 PR 수 비교에 사용.
    """

    __tablename__ = "developer_pr_events"
    __table_args__ = (
        UniqueConstraint("repo", "pr_number", name="uq_pr_event"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    repo: Mapped[str] = mapped_column(String, nullable=False, index=True)
    pr_number: Mapped[int] = mapped_column(Integer, nullable=False)
    github_login: Mapped[str] = mapped_column(String, nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    merged_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    closed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    collected_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(timezone.utc)
    )


class DeveloperTeam(Base):
    """개발자-팀 다대다 연결 테이블."""

    __tablename__ = "developer_teams"
    __table_args__ = (
        UniqueConstraint("github_login", "team", name="uq_developer_team"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    github_login: Mapped[str] = mapped_column(String, nullable=False, index=True)
    team: Mapped[str] = mapped_column(String, nullable=False, index=True)


class DeveloperPRActivity(Base):
    """수집 주기(period_start ~ period_end) 내 PR/리뷰 활동 스냅샷."""

    __tablename__ = "developer_pr_activity"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    repo: Mapped[str] = mapped_column(String, nullable=False, index=True)
    github_login: Mapped[str] = mapped_column(String, nullable=False, index=True)
    period_start: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    period_end: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    prs_opened: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    prs_merged: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    reviews_given: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    review_comments_given: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0
    )
    collected_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(timezone.utc)
    )
