"""
developer_weekly_commits 백필 스크립트.

GitHub /stats/contributors API는 레포의 전체 히스토리 주간 데이터를 반환하므로,
지금 실행하면 cron이 다운됐던 기간(4월 8~16일)의 데이터도 모두 채워진다.

실행:
    python backfill_weekly_commits.py
    python backfill_weekly_commits.py --repo SG-STAgora/some-repo  # 특정 레포만
"""

import argparse
import logging
import time
import sys
from datetime import date, datetime, timezone

import httpx

from database import SessionLocal
from github_client import _headers, fetch_org_repos
from models import DeveloperWeeklyCommits
from config import settings


def fetch_contributor_stats_patient(repo: str, max_attempts: int = 20) -> list[dict]:
    """
    backfill용 — 최대 max_attempts 회 재시도, 대기 시간을 길게 가져간다.
    GitHub stats API는 cache miss 시 202를 반환하며 수 분 후 200을 반환한다.
    """
    url = f"https://api.github.com/repos/{repo}/stats/contributors"
    for attempt in range(1, max_attempts + 1):
        with httpx.Client(timeout=30) as client:
            response = client.get(url, headers=_headers())
        if response.status_code == 202:
            wait = min(15 * attempt, 120)  # 최대 2분 대기
            logger.info("GitHub stats not ready for %s (attempt %d), waiting %ds…", repo, attempt, wait)
            time.sleep(wait)
            continue
        if response.status_code == 200:
            return response.json() or []
        response.raise_for_status()
    logger.warning("Contributor stats unavailable for %s after %d attempts", repo, max_attempts)
    return []

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("backfill")


def _is_stats_stale(stats: list[dict]) -> bool:
    total_commits = 0
    total_lines = 0
    for contributor in stats:
        for week in contributor.get("weeks", []):
            total_commits += week.get("c", 0)
            total_lines += week.get("a", 0) + week.get("d", 0)
    return total_commits > 0 and total_lines == 0


def backfill_repo(repo: str, db) -> int:
    stats = fetch_contributor_stats_patient(repo)
    if not stats:
        logger.warning("No contributor stats for %s — skipped", repo)
        return 0

    if _is_stats_stale(stats):
        logger.warning(
            "Stale contributor stats for %s (commits>0 but all lines=0) — skipped to avoid overwriting valid data",
            repo,
        )
        return 0

    now = datetime.now(timezone.utc)
    upserted = 0

    for contributor in stats:
        login: str = (contributor.get("author") or {}).get("login", "")
        if not login:
            continue

        for week in contributor.get("weeks", []):
            additions = week.get("a", 0)
            deletions = week.get("d", 0)
            commits = week.get("c", 0)
            if additions == 0 and deletions == 0 and commits == 0:
                continue

            week_start = date.fromtimestamp(week["w"])
            existing = (
                db.query(DeveloperWeeklyCommits)
                .filter(
                    DeveloperWeeklyCommits.repo == repo,
                    DeveloperWeeklyCommits.github_login == login,
                    DeveloperWeeklyCommits.week_start == week_start,
                )
                .first()
            )
            if existing:
                stale_week = additions == 0 and deletions == 0 and commits > 0
                if not stale_week or (existing.additions == 0 and existing.deletions == 0):
                    existing.additions = additions
                    existing.deletions = deletions
                existing.commits = commits
                existing.collected_at = now
            else:
                db.add(
                    DeveloperWeeklyCommits(
                        repo=repo,
                        github_login=login,
                        week_start=week_start,
                        additions=additions,
                        deletions=deletions,
                        commits=commits,
                        collected_at=now,
                    )
                )
            upserted += 1

    db.commit()
    return upserted


def main():
    parser = argparse.ArgumentParser(description="Backfill developer_weekly_commits")
    parser.add_argument("--repo", help="특정 레포만 백필 (예: SG-STAgora/my-repo)")
    args = parser.parse_args()

    db = SessionLocal()
    try:
        if args.repo:
            repos = [args.repo]
        elif settings.GITHUB_REPOS.strip():
            repos = [r.strip() for r in settings.GITHUB_REPOS.split(",") if r.strip()]
            logger.info("Using configured repo list (%d repos)", len(repos))
        else:
            repos = fetch_org_repos(settings.GITHUB_ORG)

        total = 0
        for i, repo in enumerate(repos, 1):
            logger.info("[%d/%d] Backfilling %s ...", i, len(repos), repo)
            try:
                count = backfill_repo(repo, db)
                logger.info("  → %d rows upserted", count)
                total += count
            except Exception as e:
                logger.error("  → Failed: %s", e)

        logger.info("Done. Total upserted: %d rows across %d repos", total, len(repos))
    finally:
        db.close()


if __name__ == "__main__":
    main()
