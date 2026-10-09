# nucleagent-im

Thin Go bridge between NucleAgent authentication and WuKongIM. The HTTP service listens on port 26655.

<!-- Absolute path on purpose: a ../nucleagent-docs link resolves against the checkout dir, so from a worktree (wt/<name>) it points at a nonexistent wt/nucleagent-docs. -->
Before editing, read `/home/workspace/nucleagent-workspace/nucleagent-docs/docs/nucleagent/09-workspace-frontend-maintenance.md` and the workspace `AGENTS.md`.

## Boundaries

- Keep credentials in environment variables.
- Keep this repository limited to IM bridge concerns.
- Do not add webhook handling, dispatch logic, or database models before their milestone.

## Self-Check Three

1. Inspect the working tree and staged diff for unrelated changes and credentials.
2. Run build, vet, and focused tests.
3. Run the service probes, then inspect the final diff and commit message.

测试性能：遵守工作区根 `/home/workspace/nucleagent-workspace/AGENTS.md` 的《测试性能规则》。

## Dual-Remote Sync

<!-- Absolute path on purpose, same reason as the maintenance-rules link. -->
- The `ndhy-app-web` (org) and `kwhitestone` remotes of this repository must stay identical: trunk, tags and `backup/*` branches. Identify them by URL, not remote name (`git remote -v`); a repository with a single remote pushes only to that one.
- Every push or merge to one remote's trunk must push the same SHA to the other in the same step (fast-forward; history rewrites only via backup branch + `--force-with-lease=<branch>:<old-sha>`; never bare `--force`, `--all` or `--mirror`; never drop commits).
- Closing reports paste `git ls-remote <remote> refs/heads/<trunk>` for both remotes; the work is done only when the SHAs match.
- Ignore the org CI (`verify` fails with 403 in `actions/checkout`); it never blocks a merge or a report. Use `gh pr merge --admin --rebase` when review is required.
- Full rules: [12-dual-remote-sync.md](/home/workspace/nucleagent-workspace/nucleagent-docs/docs/nucleagent/12-dual-remote-sync.md).
