import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone, date

import httpx
from apscheduler.schedulers.background import BackgroundScheduler

from config import settings
from database import SessionLocal
from github_client import (
    fetch_contributor_stats,
    fetch_merged_pull_requests,
    fetch_org_repos,
    fetch_pr_files,
    fetch_pull_requests,
    fetch_review_comments,
    fetch_reviews_for_pr,
)
from models import DeveloperMergedPRLines, DeveloperPRActivity, DeveloperPREvent, DeveloperReviewEvent, DeveloperWeeklyCommits

logger = logging.getLogger(__name__)
scheduler = BackgroundScheduler()

# PR/리뷰 수집 창 (시간). 겹침 여유를 1시간 두어 누락 방지.
PR_COLLECTION_HOURS = 25
# 리뷰 수집을 위해 per-PR 호출할 최대 PR 수 (API rate-limit 고려)
MAX_PRS_FOR_REVIEW_FETCH = 50
# PR 파일 집계를 위해 처리할 최대 PR 수
MAX_PRS_FOR_FILE_FETCH = 100


# ── Merged PR 파일 변경량 수집 ────────────────────────────────────────────────────

def _collect_merged_pr_lines(repo: str, db) -> None:
    """
    최근 PR_COLLECTION_HOURS 내 머지된 PR의 파일 변경량을 수집해 upsert.
    contrib stats가 default branch 머지분만 집계하는 제약을 보완한다.
    같은 pr_number로 이미 저장된 행이 있으면 값을 갱신(재수집 안전).
    """
    since = datetime.now(timezone.utc) - timedelta(hours=PR_COLLECTION_HOURS)
    prs = fetch_merged_pull_requests(repo, since=since)
    if not prs:
        logger.info("No merged PRs found for %s in the last %dh", repo, PR_COLLECTION_HOURS)
        return

    now = datetime.now(timezone.utc)
    upserted = 0

    with httpx.Client(timeout=20) as client:
        for pr in prs[:MAX_PRS_FOR_FILE_FETCH]:
            login: str = (pr.get("user") or {}).get("login", "")
            if not login:
                continue

            pr_number: int = pr["number"]
            base_branch: str = (pr.get("base") or {}).get("ref", "")
            merged_at = datetime.fromisoformat(
                pr["merged_at"].rstrip("Z")
            ).replace(tzinfo=timezone.utc)

            try:
                files = fetch_pr_files(repo, pr_number, client)
            except Exception as exc:
                logger.warning("Failed to fetch files for PR #%d: %s", pr_number, exc)
                continue

            total_additions = sum(f.get("additions", 0) for f in files)
            total_deletions = sum(f.get("deletions", 0) for f in files)

            existing = (
                db.query(DeveloperMergedPRLines)
                .filter(
                    DeveloperMergedPRLines.repo == repo,
                    DeveloperMergedPRLines.pr_number == pr_number,
                )
                .first()
            )
            if existing:
                existing.additions = total_additions
                existing.deletions = total_deletions
                existing.collected_at = now
            else:
                db.add(
                    DeveloperMergedPRLines(
                        repo=repo,
                        pr_number=pr_number,
                        github_login=login,
                        base_branch=base_branch,
                        additions=total_additions,
                        deletions=total_deletions,
                        merged_at=merged_at,
                        collected_at=now,
                    )
                )
            upserted += 1

    db.commit()
    logger.info("Merged PR lines upserted for %s: %d rows", repo, upserted)


# ── 커밋/라인 수집 ──────────────────────────────────────────────────────────────

def _collect_commit_stats(repo: str, db) -> None:
    """
    /stats/contributors 에서 전체 주간 기록을 받아 upsert.
    이미 저장된 주는 값을 갱신하고, 없는 주는 새로 삽입.
    """
    stats = fetch_contributor_stats(repo)
    if not stats:
        logger.warning("No contributor stats returned for %s", repo)
        return

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
    logger.info("Commit stats upserted for %s: %d rows", repo, upserted)


# ── PR/리뷰 활동 수집 ────────────────────────────────────────────────────────────

def _collect_pr_activity(repo: str, db) -> None:
    """
    period_start ~ period_end 창 내의 PR 오픈/머지, 리뷰, 리뷰 코멘트를
    개발자별로 집계해서 DeveloperPRActivity 로 저장.
    """
    period_end = datetime.now(timezone.utc)
    period_start = period_end - timedelta(hours=PR_COLLECTION_HOURS)

    prs = fetch_pull_requests(repo, since=period_start)
    review_comments = fetch_review_comments(repo, since=period_start)

    prs_opened: dict[str, int] = defaultdict(int)
    prs_merged: dict[str, int] = defaultdict(int)
    reviews_given: dict[str, int] = defaultdict(int)
    review_comments_given: dict[str, int] = defaultdict(int)

    # PR 오픈 / 머지 집계
    for pr in prs:
        login: str = (pr.get("user") or {}).get("login", "")
        if not login:
            continue

        created_at = datetime.fromisoformat(
            pr["created_at"].rstrip("Z")
        ).replace(tzinfo=timezone.utc)
        if created_at >= period_start:
            prs_opened[login] += 1

        merged_at_str = pr.get("merged_at")
        if merged_at_str:
            merged_at = datetime.fromisoformat(
                merged_at_str.rstrip("Z")
            ).replace(tzinfo=timezone.utc)
            if merged_at >= period_start:
                prs_merged[login] += 1

    # 리뷰 코멘트 집계 (bulk 엔드포인트 – 빠름)
    for comment in review_comments:
        login = (comment.get("user") or {}).get("login", "")
        if login:
            review_comments_given[login] += 1

    # 리뷰 집계 (per-PR 호출 – 상한 MAX_PRS_FOR_REVIEW_FETCH)
    pr_numbers = [pr["number"] for pr in prs[:MAX_PRS_FOR_REVIEW_FETCH]]
    with httpx.Client(timeout=15) as client:
        for pr_number in pr_numbers:
            try:
                for review in fetch_reviews_for_pr(repo, pr_number, client):
                    login = (review.get("user") or {}).get("login", "")
                    submitted_at_str = review.get("submitted_at")
                    if not login or not submitted_at_str:
                        continue
                    submitted_at = datetime.fromisoformat(
                        submitted_at_str.rstrip("Z")
                    ).replace(tzinfo=timezone.utc)
                    if submitted_at >= period_start:
                        reviews_given[login] += 1
            except Exception as exc:
                logger.warning("Failed to fetch reviews for PR #%d: %s", pr_number, exc)

    # 집계된 모든 개발자에 대해 저장
    all_logins = (
        set(prs_opened) | set(prs_merged) | set(reviews_given) | set(review_comments_given)
    )
    for login in all_logins:
        db.add(
            DeveloperPRActivity(
                repo=repo,
                github_login=login,
                period_start=period_start,
                period_end=period_end,
                prs_opened=prs_opened.get(login, 0),
                prs_merged=prs_merged.get(login, 0),
                reviews_given=reviews_given.get(login, 0),
                review_comments_given=review_comments_given.get(login, 0),
            )
        )
    db.commit()
    logger.info(
        "PR activity stored for %s: %d developers in window [%s, %s]",
        repo, len(all_logins), period_start.isoformat(), period_end.isoformat(),
    )


# ── PR 이벤트 수집 (PR당 1행, 실제 날짜 기준) ────────────────────────────────────

def _collect_pr_events(repo: str, db) -> None:
    """
    repo의 모든 PR을 수집해 developer_pr_events 테이블에 upsert.
    PR 1건 = 1행으로, created_at/merged_at/closed_at을 실제 발생 시각으로 저장.
    """
    since = datetime.now(timezone.utc) - timedelta(days=365)  # 최대 1년치
    prs = fetch_pull_requests(repo, since=since)
    upserted = 0
    for pr in prs:
        login: str = (pr.get("user") or {}).get("login", "")
        if not login:
            continue
        pr_number: int = pr["number"]
        created_at = datetime.fromisoformat(
            pr["created_at"].rstrip("Z")
        ).replace(tzinfo=timezone.utc)
        merged_at = None
        if pr.get("merged_at"):
            merged_at = datetime.fromisoformat(
                pr["merged_at"].rstrip("Z")
            ).replace(tzinfo=timezone.utc)
        closed_at = None
        if pr.get("closed_at"):
            closed_at = datetime.fromisoformat(
                pr["closed_at"].rstrip("Z")
            ).replace(tzinfo=timezone.utc)

        existing = (
            db.query(DeveloperPREvent)
            .filter(DeveloperPREvent.repo == repo, DeveloperPREvent.pr_number == pr_number)
            .first()
        )
        if existing:
            existing.merged_at = merged_at
            existing.closed_at = closed_at
            existing.collected_at = datetime.now(timezone.utc)
        else:
            db.add(DeveloperPREvent(
                repo=repo,
                pr_number=pr_number,
                github_login=login,
                created_at=created_at,
                merged_at=merged_at,
                closed_at=closed_at,
            ))
        upserted += 1

    db.commit()
    logger.info("PR events upserted for %s: %d rows", repo, upserted)


# ── 리뷰 이벤트 수집 ────────────────────────────────────────────────────────────

def _collect_review_events(repo: str, db, since: datetime) -> None:
    """
    since 이후의 리뷰 이벤트(인라인 코멘트 + PR 리뷰)를 수집해
    developer_review_events 테이블에 upsert.
    """
    now = datetime.now(timezone.utc)
    upserted = 0

    # ── 인라인 리뷰 코멘트 ──────────────────────────────────────────────────────
    review_comments = fetch_review_comments(repo, since=since)
    for comment in review_comments:
        login: str = (comment.get("user") or {}).get("login", "")
        comment_id: int = comment.get("id")
        pr_url: str = comment.get("pull_request_url", "")
        submitted_at_str: str = comment.get("created_at")
        if not login or not comment_id or not submitted_at_str:
            continue
        pr_number = 0
        if pr_url:
            try:
                pr_number = int(pr_url.rsplit("/", 1)[-1])
            except (ValueError, IndexError):
                pass
        submitted_at = datetime.fromisoformat(
            submitted_at_str.rstrip("Z")
        ).replace(tzinfo=timezone.utc)
        existing = (
            db.query(DeveloperReviewEvent)
            .filter(
                DeveloperReviewEvent.repo == repo,
                DeveloperReviewEvent.review_id == comment_id,
                DeveloperReviewEvent.review_type == "review_comment",
            )
            .first()
        )
        if not existing:
            db.add(DeveloperReviewEvent(
                repo=repo,
                review_id=comment_id,
                review_type="review_comment",
                pr_number=pr_number,
                github_login=login,
                submitted_at=submitted_at,
                collected_at=now,
            ))
            upserted += 1
    db.commit()

    # ── PR 리뷰 (approve / request_changes / comment 등) ────────────────────────
    prs = fetch_pull_requests(repo, since=since)
    with httpx.Client(timeout=15) as client:
        for pr in prs:
            pr_number: int = pr["number"]
            try:
                for review in fetch_reviews_for_pr(repo, pr_number, client):
                    login = (review.get("user") or {}).get("login", "")
                    review_id: int = review.get("id")
                    submitted_at_str = review.get("submitted_at")
                    if not login or not review_id or not submitted_at_str:
                        continue
                    submitted_at = datetime.fromisoformat(
                        submitted_at_str.rstrip("Z")
                    ).replace(tzinfo=timezone.utc)
                    if submitted_at < since:
                        continue
                    existing = (
                        db.query(DeveloperReviewEvent)
                        .filter(
                            DeveloperReviewEvent.repo == repo,
                            DeveloperReviewEvent.review_id == review_id,
                            DeveloperReviewEvent.review_type == "review",
                        )
                        .first()
                    )
                    if not existing:
                        db.add(DeveloperReviewEvent(
                            repo=repo,
                            review_id=review_id,
                            review_type="review",
                            pr_number=pr_number,
                            github_login=login,
                            submitted_at=submitted_at,
                            collected_at=now,
                        ))
                        upserted += 1
            except Exception as exc:
                logger.warning("Review events: reviews failed for PR #%d: %s", pr_number, exc)
    db.commit()
    logger.info("Review events upserted for %s: %d rows", repo, upserted)


# ── 스케줄러 진입점 ──────────────────────────────────────────────────────────────

def collect_metrics() -> None:
    try:
        repos = fetch_org_repos(settings.GITHUB_ORG)
    except Exception as exc:
        logger.error("Failed to fetch org repos for %s: %s", settings.GITHUB_ORG, exc)
        return
    if not repos:
        logger.warning("No active repos found in org %s", settings.GITHUB_ORG)
        return

    db = SessionLocal()
    try:
        for repo in repos:
            logger.info("Collecting metrics for %s", repo)
            try:
                _collect_commit_stats(repo, db)
            except Exception as exc:
                logger.error("Commit stats failed for %s: %s", repo, exc)
                db.rollback()
            try:
                _collect_merged_pr_lines(repo, db)
            except Exception as exc:
                logger.error("Merged PR lines failed for %s: %s", repo, exc)
                db.rollback()
            try:
                _collect_pr_activity(repo, db)
            except Exception as exc:
                logger.error("PR activity failed for %s: %s", repo, exc)
                db.rollback()
            try:
                _collect_pr_events(repo, db)
            except Exception as exc:
                logger.error("PR events failed for %s: %s", repo, exc)
                db.rollback()
            try:
                since_review = datetime.now(timezone.utc) - timedelta(hours=PR_COLLECTION_HOURS)
                _collect_review_events(repo, db, since=since_review)
            except Exception as exc:
                logger.error("Review events failed for %s: %s", repo, exc)
                db.rollback()
    finally:
        db.close()


def backfill_pr_data(since: datetime) -> dict:
    """
    since 이후의 PR 라인 변경량 + PR/리뷰 활동을 모든 org repo에 대해 소급 수집.
    엔드포인트에서 백그라운드 스레드로 호출되므로 DB 세션을 자체적으로 관리.
    반환값: 처리 결과 요약 dict.
    """
    try:
        repos = fetch_org_repos(settings.GITHUB_ORG)
    except Exception as exc:
        logger.error("Backfill: failed to fetch org repos: %s", exc)
        return {"error": str(exc)}

    summary: dict[str, dict] = {}
    db = SessionLocal()
    try:
        for repo in repos:
            repo_result: dict = {"pr_lines": 0, "pr_activity_devs": 0, "error": None}
            logger.info("Backfill started for %s since %s", repo, since.isoformat())

            # ── Merged PR 파일 변경량 ──────────────────────────────────────────
            try:
                prs = fetch_merged_pull_requests(repo, since=since)
                now = datetime.now(timezone.utc)
                upserted = 0
                with httpx.Client(timeout=20) as client:
                    for pr in prs:  # 백필은 상한 없이 전체 처리
                        login: str = (pr.get("user") or {}).get("login", "")
                        if not login:
                            continue
                        pr_number: int = pr["number"]
                        base_branch: str = (pr.get("base") or {}).get("ref", "")
                        merged_at = datetime.fromisoformat(
                            pr["merged_at"].rstrip("Z")
                        ).replace(tzinfo=timezone.utc)
                        try:
                            files = fetch_pr_files(repo, pr_number, client)
                        except Exception as exc:
                            logger.warning("Backfill: files failed PR #%d: %s", pr_number, exc)
                            continue
                        total_adds = sum(f.get("additions", 0) for f in files)
                        total_dels = sum(f.get("deletions", 0) for f in files)
                        existing = (
                            db.query(DeveloperMergedPRLines)
                            .filter(
                                DeveloperMergedPRLines.repo == repo,
                                DeveloperMergedPRLines.pr_number == pr_number,
                            )
                            .first()
                        )
                        if existing:
                            existing.additions = total_adds
                            existing.deletions = total_dels
                            existing.collected_at = now
                        else:
                            db.add(
                                DeveloperMergedPRLines(
                                    repo=repo,
                                    pr_number=pr_number,
                                    github_login=login,
                                    base_branch=base_branch,
                                    additions=total_adds,
                                    deletions=total_dels,
                                    merged_at=merged_at,
                                    collected_at=now,
                                )
                            )
                        upserted += 1
                db.commit()
                repo_result["pr_lines"] = upserted
                logger.info("Backfill PR lines for %s: %d rows", repo, upserted)
            except Exception as exc:
                logger.error("Backfill PR lines failed for %s: %s", repo, exc)
                db.rollback()
                repo_result["error"] = str(exc)

            # ── PR/리뷰 활동 ───────────────────────────────────────────────────
            try:
                prs_all = fetch_pull_requests(repo, since=since)
                review_comments = fetch_review_comments(repo, since=since)

                prs_opened: dict[str, int] = defaultdict(int)
                prs_merged: dict[str, int] = defaultdict(int)
                reviews_given: dict[str, int] = defaultdict(int)
                review_comments_given: dict[str, int] = defaultdict(int)

                for pr in prs_all:
                    login = (pr.get("user") or {}).get("login", "")
                    if not login:
                        continue
                    created_at = datetime.fromisoformat(
                        pr["created_at"].rstrip("Z")
                    ).replace(tzinfo=timezone.utc)
                    if created_at >= since:
                        prs_opened[login] += 1
                    merged_at_str = pr.get("merged_at")
                    if merged_at_str:
                        merged_at = datetime.fromisoformat(
                            merged_at_str.rstrip("Z")
                        ).replace(tzinfo=timezone.utc)
                        if merged_at >= since:
                            prs_merged[login] += 1

                for comment in review_comments:
                    login = (comment.get("user") or {}).get("login", "")
                    if login:
                        review_comments_given[login] += 1

                pr_numbers = [pr["number"] for pr in prs_all[:MAX_PRS_FOR_REVIEW_FETCH]]
                with httpx.Client(timeout=15) as client:
                    for pr_number in pr_numbers:
                        try:
                            for review in fetch_reviews_for_pr(repo, pr_number, client):
                                login = (review.get("user") or {}).get("login", "")
                                submitted_at_str = review.get("submitted_at")
                                if not login or not submitted_at_str:
                                    continue
                                submitted_at = datetime.fromisoformat(
                                    submitted_at_str.rstrip("Z")
                                ).replace(tzinfo=timezone.utc)
                                if submitted_at >= since:
                                    reviews_given[login] += 1
                        except Exception as exc:
                            logger.warning("Backfill: reviews failed PR #%d: %s", pr_number, exc)

                period_end = datetime.now(timezone.utc)
                all_logins = (
                    set(prs_opened) | set(prs_merged)
                    | set(reviews_given) | set(review_comments_given)
                )
                for login in all_logins:
                    db.add(
                        DeveloperPRActivity(
                            repo=repo,
                            github_login=login,
                            period_start=since,
                            period_end=period_end,
                            prs_opened=prs_opened.get(login, 0),
                            prs_merged=prs_merged.get(login, 0),
                            reviews_given=reviews_given.get(login, 0),
                            review_comments_given=review_comments_given.get(login, 0),
                        )
                    )
                db.commit()
                repo_result["pr_activity_devs"] = len(all_logins)
                logger.info("Backfill PR activity for %s: %d developers", repo, len(all_logins))
            except Exception as exc:
                logger.error("Backfill PR activity failed for %s: %s", repo, exc)
                db.rollback()
                if repo_result["error"]:
                    repo_result["error"] += f" | {exc}"
                else:
                    repo_result["error"] = str(exc)

            # ── 리뷰 이벤트 ─────────────────────────────────────────────────────
            try:
                _collect_review_events(repo, db, since=since)
                repo_result["review_events"] = True
            except Exception as exc:
                logger.error("Backfill review events failed for %s: %s", repo, exc)
                db.rollback()

            summary[repo] = repo_result
    finally:
        db.close()

    logger.info("Backfill complete: %s", summary)
    return summary


def backfill_weekly_commits() -> dict:
    """
    /stats/contributors API로 전체 주간 히스토리를 모든 org repo에 대해 upsert.
    GitHub stats API는 전체 히스토리를 반환하므로 since 파라미터 불필요.
    엔드포인트에서 백그라운드 스레드로 호출.
    """
    try:
        repos = fetch_org_repos(settings.GITHUB_ORG)
    except Exception as exc:
        logger.error("backfill_weekly_commits: failed to fetch repos: %s", exc)
        return {"error": str(exc)}

    total_upserted = 0
    skipped = []
    db = SessionLocal()
    try:
        now = datetime.now(timezone.utc)
        for repo in repos:
            try:
                stats = fetch_contributor_stats(repo)
            except Exception as exc:
                logger.error("backfill_weekly_commits: fetch failed for %s: %s", repo, exc)
                skipped.append(repo)
                continue
            if not stats:
                logger.warning("backfill_weekly_commits: no stats for %s — skipped", repo)
                skipped.append(repo)
                continue

            upserted = 0
            try:
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
                logger.info("backfill_weekly_commits: %s → %d rows", repo, upserted)
                total_upserted += upserted
            except Exception as exc:
                logger.error("backfill_weekly_commits: upsert failed for %s: %s", repo, exc)
                db.rollback()
                skipped.append(repo)
    finally:
        db.close()

    logger.info("backfill_weekly_commits done: %d total rows, %d repos skipped", total_upserted, len(skipped))
    return {"total_upserted": total_upserted, "skipped_repos": skipped}


def start_scheduler() -> None:
    # 기동 직후 즉시 1회 실행
    scheduler.add_job(collect_metrics, "date", id="collect_metrics_startup")
    # 이후 24시간 간격 반복 실행
    scheduler.add_job(collect_metrics, "interval", hours=24, id="collect_metrics_daily")
    scheduler.start()
    logger.info("Scheduler started – immediate run on startup, then every 24 hours")


def stop_scheduler() -> None:
    if scheduler.running:
        scheduler.shutdown()
        logger.info("Scheduler stopped")
