import logging
import time
from datetime import datetime, timezone

import httpx

from config import settings

logger = logging.getLogger(__name__)

_MAX_PAGES = 20  # 페이지네이션 안전 상한

CONTRIBUTOR_STATS_READY = "ready"
CONTRIBUTOR_STATS_PENDING = "pending"
CONTRIBUTOR_STATS_EMPTY = "empty"
CONTRIBUTOR_STATS_ERROR = "error"


def _headers() -> dict:
    return {
        "Authorization": f"Bearer {settings.ALLOWED_GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def fetch_org_repos(org: str) -> list[str]:
    """
    GET /orgs/{org}/repos
    org 내 모든 저장소의 full_name("owner/repo") 목록을 반환.
    archived/disabled 저장소는 제외.
    """
    results: list[str] = []
    with httpx.Client(timeout=30) as client:
        for page in range(1, _MAX_PAGES + 1):
            response = client.get(
                f"https://api.github.com/orgs/{org}/repos",
                headers=_headers(),
                params={"per_page": 100, "page": page, "type": "all"},
            )
            response.raise_for_status()
            page_data: list[dict] = response.json()
            if not page_data:
                break
            for repo in page_data:
                if not repo.get("archived") and not repo.get("disabled"):
                    results.append(repo["full_name"])
            if len(page_data) < 100:
                break
    logger.info("Found %d active repos in org %s", len(results), org)
    return results


def fetch_contributor_stats(repo: str) -> list[dict]:
    """
    GET /repos/{repo}/stats/contributors
    GitHub가 통계를 계산 중이면 202를 반환하므로 최대 3회 재시도.
    반환: [{"author": {"login": ...}, "weeks": [{"w": unix_ts, "a": adds, "d": dels, "c": commits}]}]
    """
    status, data = fetch_contributor_stats_with_status(repo, max_attempts=3, initial_delay=5)
    if status == CONTRIBUTOR_STATS_PENDING:
        logger.warning("Contributor stats unavailable for %s after retries", repo)
        return []
    if status == CONTRIBUTOR_STATS_ERROR:
        logger.warning("Contributor stats request failed for %s", repo)
        return []
    return data


def fetch_contributor_stats_with_status(
    repo: str,
    max_attempts: int = 3,
    initial_delay: int = 5,
    max_delay: int = 60,
) -> tuple[str, list[dict]]:
    """
    /stats/contributors 결과를 상태와 함께 반환.
    반환 상태:
      - ready: 통계 데이터 준비됨
      - empty: 통계 없음(204 또는 빈 바디)
      - pending: 아직 계산 중(202)
      - error: 요청 실패 또는 응답 파싱 실패
    """
    url = f"https://api.github.com/repos/{repo}/stats/contributors"
    delay = max(1, initial_delay)

    for attempt in range(max_attempts):
        try:
            with httpx.Client(timeout=30) as client:
                response = client.get(url, headers=_headers())
        except Exception as exc:
            logger.warning("Contributor stats request error for %s: %s", repo, exc)
            return CONTRIBUTOR_STATS_ERROR, []

        if response.status_code == 202:
            logger.info("GitHub stats not ready for %s (attempt %d), retrying...", repo, attempt + 1)
            if attempt < max_attempts - 1:
                time.sleep(delay)
                delay = min(delay * 2, max_delay)
            continue

        if response.status_code == 204:
            return CONTRIBUTOR_STATS_EMPTY, []

        try:
            response.raise_for_status()
        except Exception as exc:
            logger.warning("Contributor stats HTTP error for %s: %s", repo, exc)
            return CONTRIBUTOR_STATS_ERROR, []

        if not response.content:
            return CONTRIBUTOR_STATS_EMPTY, []

        try:
            data = response.json() or []
        except Exception as exc:
            logger.warning("Contributor stats JSON parse error for %s: %s", repo, exc)
            return CONTRIBUTOR_STATS_ERROR, []

        if data:
            return CONTRIBUTOR_STATS_READY, data
        return CONTRIBUTOR_STATS_EMPTY, []

    return CONTRIBUTOR_STATS_PENDING, []


def fetch_repo_commits(
    repo: str,
    since: datetime | None = None,
    until: datetime | None = None,
    max_pages: int = _MAX_PAGES,
) -> list[dict]:
    """
    GET /repos/{repo}/commits
    커밋 목록을 페이지네이션으로 수집.
    기본 브랜치 기준으로 반환되며, 작성자 login이 없는 커밋은 호출 측에서 필터링한다.
    """
    params_base: dict[str, str | int] = {"per_page": 100}
    if since:
        params_base["since"] = since.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    if until:
        params_base["until"] = until.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    results: list[dict] = []
    with httpx.Client(timeout=30) as client:
        for page in range(1, max_pages + 1):
            params = dict(params_base)
            params["page"] = page
            response = client.get(
                f"https://api.github.com/repos/{repo}/commits",
                headers=_headers(),
                params=params,
            )
            response.raise_for_status()
            page_data: list[dict] = response.json() or []
            if not page_data:
                break
            results.extend(page_data)
            if len(page_data) < 100:
                break
    return results


def fetch_pull_requests(repo: str, since: datetime) -> list[dict]:
    """
    GET /repos/{repo}/pulls?state=all  (updated 순 내림차순)
    since 이전에 마지막으로 업데이트된 PR이 나오면 조기 종료.
    """
    results: list[dict] = []
    with httpx.Client(timeout=30) as client:
        for page in range(1, _MAX_PAGES + 1):
            response = client.get(
                f"https://api.github.com/repos/{repo}/pulls",
                headers=_headers(),
                params={"state": "all", "per_page": 100, "page": page,
                        "sort": "updated", "direction": "desc"},
            )
            response.raise_for_status()
            page_data: list[dict] = response.json()
            if not page_data:
                break
            stop = False
            for pr in page_data:
                updated_at = datetime.fromisoformat(
                    pr["updated_at"].rstrip("Z")
                ).replace(tzinfo=timezone.utc)
                if updated_at < since:
                    stop = True
                    break
                results.append(pr)
            if stop or len(page_data) < 100:
                break
    return results


def fetch_review_comments(repo: str, since: datetime) -> list[dict]:
    """
    GET /repos/{repo}/pulls/comments?since=...
    since 이후의 리뷰 인라인 코멘트를 일괄 수집.
    """
    since_str = since.strftime("%Y-%m-%dT%H:%M:%SZ")
    results: list[dict] = []
    with httpx.Client(timeout=30) as client:
        for page in range(1, _MAX_PAGES + 1):
            response = client.get(
                f"https://api.github.com/repos/{repo}/pulls/comments",
                headers=_headers(),
                params={"since": since_str, "per_page": 100, "page": page},
            )
            response.raise_for_status()
            page_data: list[dict] = response.json()
            if not page_data:
                break
            results.extend(page_data)
            if len(page_data) < 100:
                break
    return results


def fetch_reviews_for_pr(repo: str, pr_number: int, client: httpx.Client) -> list[dict]:
    """
    GET /repos/{repo}/pulls/{number}/reviews
    단일 PR에 대한 리뷰 목록 반환. 외부에서 Client를 주입받아 커넥션 재사용.
    """
    response = client.get(
        f"https://api.github.com/repos/{repo}/pulls/{pr_number}/reviews",
        headers=_headers(),
        params={"per_page": 100},
    )
    response.raise_for_status()
    return response.json() or []


def fetch_pr_files(repo: str, pr_number: int, client: httpx.Client) -> list[dict]:
    """
    GET /repos/{repo}/pulls/{number}/files
    PR에서 변경된 파일 목록(additions/deletions 포함)을 반환.
    최대 300개 파일까지 페이지네이션으로 수집 (GitHub API 상한 3000 파일이나 실용 상한 적용).
    """
    results: list[dict] = []
    for page in range(1, 4):  # 최대 300개 파일
        response = client.get(
            f"https://api.github.com/repos/{repo}/pulls/{pr_number}/files",
            headers=_headers(),
            params={"per_page": 100, "page": page},
        )
        response.raise_for_status()
        page_data: list[dict] = response.json()
        if not page_data:
            break
        results.extend(page_data)
        if len(page_data) < 100:
            break
    return results


def fetch_org_team_members(org: str) -> dict[str, list[str]]:
    """
    GET /orgs/{org}/teams  →  각 팀의 GET /orgs/{org}/teams/{slug}/members
    팀 slug → github_login 목록 매핑을 반환.
    토큰에 read:org 스코프가 필요합니다.
    """
    team_map: dict[str, list[str]] = {}
    with httpx.Client(timeout=30) as client:
        # 팀 목록 수집
        teams: list[dict] = []
        for page in range(1, _MAX_PAGES + 1):
            response = client.get(
                f"https://api.github.com/orgs/{org}/teams",
                headers=_headers(),
                params={"per_page": 100, "page": page},
            )
            response.raise_for_status()
            page_data: list[dict] = response.json()
            if not page_data:
                break
            teams.extend(page_data)
            if len(page_data) < 100:
                break

        # 각 팀 멤버 수집
        for team in teams:
            slug: str = team["slug"]
            name: str = team["name"]
            members: list[str] = []
            for page in range(1, _MAX_PAGES + 1):
                response = client.get(
                    f"https://api.github.com/orgs/{org}/teams/{slug}/members",
                    headers=_headers(),
                    params={"per_page": 100, "page": page},
                )
                response.raise_for_status()
                page_data = response.json()
                if not page_data:
                    break
                members.extend(m["login"] for m in page_data)
                if len(page_data) < 100:
                    break
            team_map[name] = members
            logger.info("Team %s (%s): %d members", name, slug, len(members))

    return team_map


def fetch_user_names(logins: list[str]) -> dict[str, str | None]:
    """
    GET /users/{login} 을 각 login에 대해 호출하여 프로필 표시 이름(name)을 반환.
    name이 없는 경우 None.
    """
    result: dict[str, str | None] = {}
    with httpx.Client(timeout=30) as client:
        for login in logins:
            response = client.get(
                f"https://api.github.com/users/{login}",
                headers=_headers(),
            )
            if response.status_code == 404:
                result[login] = None
                continue
            response.raise_for_status()
            result[login] = response.json().get("name")  # 프로필 표시 이름, 미설정 시 None
    logger.info("Fetched profile names for %d users", len(result))
    return result


def fetch_merged_pull_requests(repo: str, since: datetime) -> list[dict]:
    """
    GET /repos/{repo}/pulls?state=closed&sort=updated&direction=desc
    since 이후 merged_at 이 있는(= 실제 머지된) PR 목록만 반환.
    """
    results: list[dict] = []
    with httpx.Client(timeout=30) as client:
        for page in range(1, _MAX_PAGES + 1):
            response = client.get(
                f"https://api.github.com/repos/{repo}/pulls",
                headers=_headers(),
                params={
                    "state": "closed",
                    "per_page": 100,
                    "page": page,
                    "sort": "updated",
                    "direction": "desc",
                },
            )
            response.raise_for_status()
            page_data: list[dict] = response.json()
            if not page_data:
                break
            stop = False
            for pr in page_data:
                updated_at = datetime.fromisoformat(
                    pr["updated_at"].rstrip("Z")
                ).replace(tzinfo=timezone.utc)
                if updated_at < since:
                    stop = True
                    break
                if pr.get("merged_at"):  # closed 이지만 머지되지 않은 PR 제외
                    results.append(pr)
            if stop or len(page_data) < 100:
                break
    return results
