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
