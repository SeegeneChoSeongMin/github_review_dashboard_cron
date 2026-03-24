import logging
from datetime import datetime, timezone

from apscheduler.schedulers.background import BackgroundScheduler

from config import settings
from database import SessionLocal
from github_client import fetch_repo_metrics
from models import RepoMetrics

logger = logging.getLogger(__name__)

scheduler = BackgroundScheduler()


def collect_metrics():
    logger.info("Collecting GitHub metrics for %s", settings.GITHUB_REPO)
    try:
        data = fetch_repo_metrics()
    except Exception as exc:
        logger.error("Failed to fetch GitHub metrics: %s", exc)
        return

    db = SessionLocal()
    try:
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
        logger.info("Metrics stored: stars=%d forks=%d", record.stars, record.forks)
    except Exception as exc:
        logger.error("Failed to store metrics: %s", exc)
        db.rollback()
    finally:
        db.close()


def start_scheduler():
    scheduler.add_job(collect_metrics, "interval", hours=1, id="collect_metrics")
    scheduler.start()
    logger.info("Scheduler started – collecting metrics every hour")


def stop_scheduler():
    if scheduler.running:
        scheduler.shutdown()
        logger.info("Scheduler stopped")
