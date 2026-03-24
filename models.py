from datetime import datetime, timezone

from sqlalchemy import DateTime, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from database import Base


class RepoMetrics(Base):
    __tablename__ = "repo_metrics"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    repo: Mapped[str] = mapped_column(String, nullable=False, index=True)
    stars: Mapped[int] = mapped_column(Integer, nullable=False)
    forks: Mapped[int] = mapped_column(Integer, nullable=False)
    open_issues: Mapped[int] = mapped_column(Integer, nullable=False)
    watchers: Mapped[int] = mapped_column(Integer, nullable=False)
    collected_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(timezone.utc)
    )
