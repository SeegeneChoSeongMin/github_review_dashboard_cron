import httpx

from config import settings
from schemas import GitHubRepoData


def fetch_repo_metrics(repo: str | None = None) -> GitHubRepoData:
    target_repo = repo or settings.GITHUB_REPO
    url = f"https://api.github.com/repos/{target_repo}"
    headers = {
        "Authorization": f"Bearer {settings.GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    with httpx.Client(timeout=10) as client:
        response = client.get(url, headers=headers)
    response.raise_for_status()
    return GitHubRepoData(**response.json())
