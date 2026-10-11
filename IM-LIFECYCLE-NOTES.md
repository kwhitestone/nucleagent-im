# IM lifecycle handoff

CLAIM: IM-only backend worker, branch feat/im-agent-impl-backend, base a82f9c7.

## Requested Core interface

POST /api/v1/addons/conversation/im-lifecycle

Same service-principal authorization and raw JSON transport as im-dispatch.
Request: {agentUid, executionOwnerUserId, sourceKey, admittedAt}.
admittedAt is the immutable IM inbox CreatedAt in RFC3339Nano UTC, never retry time.
Success: {"allowed":true}. Fail closed on missing/false allowed or any error.
403/409 response: {"code":"im_agent_binding_disabled"} or
{"code":"im_agent_lifecycle_stale"}; no private configuration in the response.

Core must resolve the current binding without creating an instance and check the
definition (including legacy instance bindings), tombstone, enablement and owner.
It must retain a durable disable/delete barrier across restore and re-enable, and
deny admittedAt at/before that barrier. Missing/zero admission time must be denied.
This is necessary even if IM was offline for the entire disable/restore interval.
Ordinary definition edits must not move this lifecycle barrier.
Core im-dispatch must enforce the same guard atomically at admission (accept
admittedAt as an additional request field); Core owns cancellation and queued,
clarification/background execution gates. IM does not promise to cancel compute.

IM rechecks local Auth state, target agent account type, human origin/owner and
group sender/target/origin membership plus invocation permission, then calls Core
before dispatch, final delivery, Core streaming snapshots and browser snapshots.
Policy-denied inbox work is terminal; infrastructure failure never permits delivery.
No definition copies, admin credentials or new privileged admin routes.

Directory invalidation uses fresh authoritative reads and Cache-Control: no-store;
there is no server directory cache to invalidate. Frontend polling is not IM-owned.

## Boundaries

Read workspace AGENTS, repo AGENTS, docs/nucleagent/09-workspace-frontend-maintenance.md,
11-test-performance.md, task impl-common.md/c1-common.md, /tmp/api-contract.md,
approved FINAL report and its definition-merged.md attachment, and ponytail skill.
12-dual-remote-sync.md is absent; workspace fallback read. No push/deploy is allowed.
All tests use local fixtures/mocks; no paid calls or shared dependency edits.
