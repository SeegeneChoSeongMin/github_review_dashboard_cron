import type { DashboardData, DeveloperStats } from "@/types/dashboard";
import { Octokit } from "@octokit/rest";

import { paginate } from "./pagination";

type ContributorStats = Extract<
  Awaited<ReturnType<Octokit["repos"]["getContributorsStats"]>>["data"],
  Array<unknown>
>;

type DashboardComputation = {
  contributorStats: ContributorStats;
  branchCommitsByLogin: Record<string, number>;
  branchLinesAdded: Record<string, number>;
  branchLinesDeleted: Record<string, number>;
};

// 대시보드 화면에 필요한 저장소 활동 데이터를 수집하고 개발자 단위로 집계한다.
export async function fetchDashboardData(
  token: string,
  owner: string,
  repo: string,
  branch?: string | null,
  since?: string | null,
  until?: string | null,
): Promise<DashboardData> {
  const octokit = new Octokit({ auth: token });

  const [{ data: repoData }, allBranches] = await Promise.all([
    octokit.repos.get({ owner, repo }),
    paginate(
      (page) =>
        octokit.repos
          .listBranches({ owner, repo, per_page: 100, page })
          .then((response) => response.data),
      5,
    ),
  ]);

  const defaultBranch = repoData.default_branch;
  const branchNames = allBranches.map((candidate) => candidate.name);
  const allPRsRaw = await fetchPullRequests(
    octokit,
    owner,
    repo,
    branch,
    defaultBranch,
  );
  const allPRs = filterPullRequestsByDate(allPRsRaw, since, until);

  const {
    contributorStats,
    branchCommitsByLogin,
    branchLinesAdded,
    branchLinesDeleted,
  } = await collectDashboardComputation({
    octokit,
    owner,
    repo,
    branch,
    since,
    until,
    defaultBranch,
    branchNames,
    allPRs,
  });

  const reviewComments = await fetchReviewComments(octokit, owner, repo, since);
  const reviewsByUser = await fetchReviewsByUser(octokit, owner, repo, allPRs);
  const developerMap = await buildDeveloperMap({
    octokit,
    owner,
    repo,
    allPRs,
    contributorStats,
    branchCommitsByLogin,
    branchLinesAdded,
    branchLinesDeleted,
    reviewComments,
    reviewsByUser,
    since,
    until,
  });

  const developers = Object.values(developerMap).sort(
    (left, right) => right.commits - left.commits,
  );

  return {
    repo: {
      owner,
      name: repo,
      fullName: repoData.full_name,
      description: repoData.description,
      stargazersCount: repoData.stargazers_count,
      openPRsCount: allPRs.filter((pullRequest) => pullRequest.state === "open")
        .length,
      totalPRsCount: allPRs.length,
      branches: branchNames,
      defaultBranch,
    },
    developers,
    lastUpdated: new Date().toISOString(),
  };
}

// 선택된 브랜치 규칙에 맞춰 PR 목록을 모두 가져온다.
async function fetchPullRequests(
  octokit: Octokit,
  owner: string,
  repo: string,
  branch: string | null | undefined,
  defaultBranch: string,
) {
  return paginate(
    (page) =>
      octokit.pulls
        .list({
          owner,
          repo,
          state: "all",
          per_page: 100,
          page,
          ...(branch === "__all__" ? {} : { base: branch || defaultBranch }),
        })
        .then((response) => response.data),
    20,
  );
}

// 조회 기간 기준으로 PR 목록을 후처리 필터링한다.
function filterPullRequestsByDate<
  T extends { merged_at: string | null; created_at: string },
>(pullRequests: T[], since?: string | null, until?: string | null): T[] {
  return pullRequests.filter((pullRequest) => {
    const date = pullRequest.merged_at ?? pullRequest.created_at;
    if (since && date < since) {
      return false;
    }
    if (until && date > until) {
      return false;
    }
    return true;
  });
}

// 브랜치 선택 방식에 따라 커밋 수와 라인 증감 집계용 원천 데이터를 수집한다.
async function collectDashboardComputation({
  octokit,
  owner,
  repo,
  branch,
  since,
  until,
  defaultBranch,
  branchNames,
  allPRs,
}: {
  octokit: Octokit;
  owner: string;
  repo: string;
  branch?: string | null;
  since?: string | null;
  until?: string | null;
  defaultBranch: string;
  branchNames: string[];
  allPRs: Awaited<ReturnType<typeof fetchPullRequests>>;
}): Promise<DashboardComputation> {
  let contributorStats: ContributorStats = [];
  const branchCommitsByLogin: Record<string, number> = {};
  const branchLinesAdded: Record<string, number> = {};
  const branchLinesDeleted: Record<string, number> = {};

  const fetchContributorStats = async (): Promise<void> => {
    contributorStats = await waitForContributorStats(octokit, owner, repo);
  };

  if (branch === "__all__") {
    await fetchContributorStats();
    await collectAllBranchCommits({
      octokit,
      owner,
      repo,
      branchNames,
      since,
      until,
      branchCommitsByLogin,
    });
    await accumulatePullRequestLineStats(
      octokit,
      owner,
      repo,
      allPRs
        .filter(
          (pullRequest) =>
            pullRequest.merged_at !== null &&
            pullRequest.base.ref !== defaultBranch,
        )
        .slice(0, 50),
      branchLinesAdded,
      branchLinesDeleted,
    );
  } else if (!branch || branch === defaultBranch) {
    await fetchContributorStats();
    if (since || until) {
      const filteredCommits = await fetchBranchCommits(
        octokit,
        owner,
        repo,
        branch || defaultBranch,
        since,
        until,
      );
      accumulateCommitCounts(filteredCommits, branchCommitsByLogin);
    }
  } else {
    const mergedPRsForBranch = allPRs
      .filter((pullRequest) => pullRequest.merged_at !== null)
      .slice(0, 50);

    const [branchCommits] = await Promise.all([
      fetchBranchCommits(octokit, owner, repo, branch, since, until),
      accumulatePullRequestLineStats(
        octokit,
        owner,
        repo,
        mergedPRsForBranch,
        branchLinesAdded,
        branchLinesDeleted,
      ),
    ]);

    accumulateCommitCounts(branchCommits, branchCommitsByLogin);
  }

  return {
    contributorStats,
    branchCommitsByLogin,
    branchLinesAdded,
    branchLinesDeleted,
  };
}

// GitHub가 비동기로 계산하는 contributor stats가 준비될 때까지 재시도한다.
async function waitForContributorStats(
  octokit: Octokit,
  owner: string,
  repo: string,
): Promise<ContributorStats> {
  for (let attempt = 0; attempt < 5; attempt++) {
    const statsResponse = await octokit.repos.getContributorsStats({
      owner,
      repo,
    });
    if (statsResponse.status === 200 && Array.isArray(statsResponse.data)) {
      return statsResponse.data;
    }
    await new Promise((resolve) => setTimeout(resolve, 2000));
  }

  return [];
}

// 특정 브랜치의 커밋 목록을 기간 조건과 함께 전부 가져온다.
async function fetchBranchCommits(
  octokit: Octokit,
  owner: string,
  repo: string,
  branch: string,
  since?: string | null,
  until?: string | null,
) {
  return paginate(
    (page) =>
      octokit.repos
        .listCommits({
          owner,
          repo,
          sha: branch,
          per_page: 100,
          page,
          ...(since ? { since } : {}),
          ...(until ? { until } : {}),
        })
        .then((response) => response.data),
    20,
  );
}

// 여러 브랜치의 커밋을 SHA 기준으로 중복 제거해 개발자별 커밋 수를 집계한다.
async function collectAllBranchCommits({
  octokit,
  owner,
  repo,
  branchNames,
  since,
  until,
  branchCommitsByLogin,
}: {
  octokit: Octokit;
  owner: string;
  repo: string;
  branchNames: string[];
  since?: string | null;
  until?: string | null;
  branchCommitsByLogin: Record<string, number>;
}): Promise<void> {
  const seenShas = new Set<string>();
  const allCommitArrays = await Promise.all(
    branchNames.slice(0, 15).map((branchName) =>
      paginate(
        (page) =>
          octokit.repos
            .listCommits({
              owner,
              repo,
              sha: branchName,
              per_page: 100,
              page,
              ...(since ? { since } : {}),
              ...(until ? { until } : {}),
            })
            .then((response) => response.data),
        5,
      ),
    ),
  );

  for (const commits of allCommitArrays) {
    for (const commit of commits) {
      if (seenShas.has(commit.sha)) {
        continue;
      }
      seenShas.add(commit.sha);

      const login = commit.author?.login;
      if (login) {
        branchCommitsByLogin[login] = (branchCommitsByLogin[login] ?? 0) + 1;
      }
    }
  }
}

// 커밋 목록을 개발자별 커밋 수 맵으로 누적 반영한다.
function accumulateCommitCounts(
  commits: Array<{ author?: { login?: string | null } | null }>,
  branchCommitsByLogin: Record<string, number>,
): void {
  for (const commit of commits) {
    const login = commit.author?.login;
    if (login) {
      branchCommitsByLogin[login] = (branchCommitsByLogin[login] ?? 0) + 1;
    }
  }
}

// PR 파일 목록을 바탕으로 작성자별 추가/삭제 라인 수를 누적한다.
async function accumulatePullRequestLineStats(
  octokit: Octokit,
  owner: string,
  repo: string,
  pullRequests: Array<{
    number: number;
    user?: { login?: string | null } | null;
  }>,
  branchLinesAdded: Record<string, number>,
  branchLinesDeleted: Record<string, number>,
): Promise<void> {
  await Promise.all(
    pullRequests.map(async (pullRequest) => {
      const login = pullRequest.user?.login;
      if (!login) {
        return;
      }

      try {
        const files = await paginate(
          (page) =>
            octokit.pulls
              .listFiles({
                owner,
                repo,
                pull_number: pullRequest.number,
                per_page: 100,
                page,
              })
              .then((response) => response.data),
          5,
        );

        for (const file of files) {
          branchLinesAdded[login] =
            (branchLinesAdded[login] ?? 0) + (file.additions ?? 0);
          branchLinesDeleted[login] =
            (branchLinesDeleted[login] ?? 0) + (file.deletions ?? 0);
        }
      } catch {
        // 개별 PR 실패는 전체 집계를 중단시키지 않는다.
      }
    }),
  );
}

// 저장소 전체 리뷰 코멘트를 가져와 개발자별 코멘트 수 집계에 사용한다.
async function fetchReviewComments(
  octokit: Octokit,
  owner: string,
  repo: string,
  since?: string | null,
) {
  return paginate(
    (page) =>
      octokit.pulls
        .listReviewCommentsForRepo({
          owner,
          repo,
          per_page: 100,
          page,
          sort: "created",
          direction: "desc",
          ...(since ? { since } : {}),
        })
        .then((response) => response.data),
    20,
  );
}

// 최근 PR 리뷰 목록을 조회해 리뷰를 수행한 사용자별 횟수를 계산한다.
async function fetchReviewsByUser(
  octokit: Octokit,
  owner: string,
  repo: string,
  allPRs: Awaited<ReturnType<typeof fetchPullRequests>>,
): Promise<Record<string, number>> {
  const openPRNumbers = allPRs
    .filter((pullRequest) => pullRequest.state === "open")
    .map((pullRequest) => pullRequest.number);
  const closedPRNumbers = allPRs
    .filter((pullRequest) => pullRequest.state === "closed")
    .slice(0, 100)
    .map((pullRequest) => pullRequest.number);
  const prNumbersForReviews = [...openPRNumbers, ...closedPRNumbers].slice(
    0,
    100,
  );

  const reviewsByUser: Record<string, number> = {};
  await Promise.all(
    prNumbersForReviews.map(async (pullRequestNumber) => {
      try {
        const { data: reviews } = await octokit.pulls.listReviews({
          owner,
          repo,
          pull_number: pullRequestNumber,
          per_page: 100,
        });

        for (const review of reviews) {
          if (
            review.user?.login &&
            review.state !== "PENDING" &&
            review.state !== "DISMISSED"
          ) {
            reviewsByUser[review.user.login] =
              (reviewsByUser[review.user.login] || 0) + 1;
          }
        }
      } catch {
        // 개별 PR 리뷰 조회 실패는 무시한다.
      }
    }),
  );

  return reviewsByUser;
}

// 집계에 필요한 개발자 엔트리를 없으면 생성하고, 있으면 기존 객체를 유지한다.
function ensureDeveloper(
  developerMap: Record<string, DeveloperStats>,
  login: string,
  avatarUrl: string,
  name?: string | null,
): void {
  if (!developerMap[login]) {
    developerMap[login] = {
      login,
      avatarUrl,
      name: name ?? null,
      linesAdded: 0,
      linesDeleted: 0,
      commits: 0,
      prsCreated: 0,
      prsMerged: 0,
      prsOpen: 0,
      reviewsGiven: 0,
      reviewComments: 0,
      openPRsWaiting: 0,
    };
  }
}

// 사용자 프로필이 비어 있는 개발자에 대해 GitHub 사용자 정보를 보강한다.
async function ensureDeveloperWithProfile(
  octokit: Octokit,
  developerMap: Record<string, DeveloperStats>,
  login: string,
): Promise<void> {
  if (developerMap[login]) {
    return;
  }

  try {
    const { data: userInfo } = await octokit.users.getByUsername({
      username: login,
    });
    ensureDeveloper(developerMap, login, userInfo.avatar_url, userInfo.name);
  } catch {
    ensureDeveloper(developerMap, login, "", null);
  }
}

// contributor stats의 주간 데이터를 조회 기간 기준으로 개발자 맵에 반영한다.
function applyContributorStats(
  contributorStats: ContributorStats,
  developerMap: Record<string, DeveloperStats>,
  since?: string | null,
  until?: string | null,
): void {
  const sinceMs = since ? new Date(since).getTime() : null;
  const untilMs = until ? new Date(until).getTime() : null;

  for (const stat of contributorStats) {
    if (!stat.author?.login) {
      continue;
    }

    const login = stat.author.login;
    ensureDeveloper(developerMap, login, stat.author.avatar_url, null);

    let totalAdded = 0;
    let totalDeleted = 0;
    let totalCommits = 0;
    for (const week of stat.weeks) {
      const weekMs = (week.w ?? 0) * 1000;
      if (sinceMs && weekMs < sinceMs) {
        continue;
      }
      if (untilMs && weekMs > untilMs) {
        continue;
      }
      totalAdded += week.a ?? 0;
      totalDeleted += week.d ?? 0;
      totalCommits += week.c ?? 0;
    }

    developerMap[login].linesAdded = totalAdded;
    developerMap[login].linesDeleted = totalDeleted;
    developerMap[login].commits = totalCommits;
  }
}

// 보조 집계 맵들을 개발자 엔트리에 합쳐 최종 개발자 상태를 만든다.
async function buildDeveloperMap({
  octokit,
  allPRs,
  contributorStats,
  branchCommitsByLogin,
  branchLinesAdded,
  branchLinesDeleted,
  reviewComments,
  reviewsByUser,
  since,
  until,
}: {
  octokit: Octokit;
  owner: string;
  repo: string;
  allPRs: Awaited<ReturnType<typeof fetchPullRequests>>;
  contributorStats: ContributorStats;
  branchCommitsByLogin: Record<string, number>;
  branchLinesAdded: Record<string, number>;
  branchLinesDeleted: Record<string, number>;
  reviewComments: Awaited<ReturnType<typeof fetchReviewComments>>;
  reviewsByUser: Record<string, number>;
  since?: string | null;
  until?: string | null;
}): Promise<Record<string, DeveloperStats>> {
  const developerMap: Record<string, DeveloperStats> = {};

  applyContributorStats(contributorStats, developerMap, since, until);

  for (const [login, count] of Object.entries(branchCommitsByLogin)) {
    await ensureDeveloperWithProfile(octokit, developerMap, login);
    developerMap[login].commits = count;
  }

  for (const [login, added] of Object.entries(branchLinesAdded)) {
    ensureDeveloper(developerMap, login, "", null);
    developerMap[login].linesAdded += added;
  }
  for (const [login, deleted] of Object.entries(branchLinesDeleted)) {
    ensureDeveloper(developerMap, login, "", null);
    developerMap[login].linesDeleted += deleted;
  }

  for (const pullRequest of allPRs) {
    const login = pullRequest.user?.login;
    if (!login) {
      continue;
    }
    ensureDeveloper(
      developerMap,
      login,
      pullRequest.user?.avatar_url ?? "",
      null,
    );
    developerMap[login].prsCreated += 1;
    if (pullRequest.state === "closed" && pullRequest.merged_at) {
      developerMap[login].prsMerged += 1;
    }
    if (pullRequest.state === "open") {
      developerMap[login].prsOpen += 1;
    }
  }

  const reviewRequestsByUser: Record<string, number> = {};
  for (const pullRequest of allPRs.filter(
    (candidate) => candidate.state === "open",
  )) {
    for (const reviewer of pullRequest.requested_reviewers ?? []) {
      if (reviewer.login) {
        reviewRequestsByUser[reviewer.login] =
          (reviewRequestsByUser[reviewer.login] || 0) + 1;
      }
    }
  }

  for (const comment of reviewComments) {
    const login = comment.user?.login;
    if (!login) {
      continue;
    }
    ensureDeveloper(developerMap, login, comment.user?.avatar_url ?? "", null);
    developerMap[login].reviewComments += 1;
  }

  for (const [login, count] of Object.entries(reviewsByUser)) {
    await ensureDeveloperWithProfile(octokit, developerMap, login);
    developerMap[login].reviewsGiven = count;
  }

  for (const [login, count] of Object.entries(reviewRequestsByUser)) {
    await ensureDeveloperWithProfile(octokit, developerMap, login);
    developerMap[login].openPRsWaiting = count;
  }

  return developerMap;
}
