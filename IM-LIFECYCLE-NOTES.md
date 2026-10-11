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

## Public directory contract (frontend follow-up)

GET /api/v1/im/agents keeps the existing {code,message,data:[...]} envelope.
Each item: {uid,name,description,avatar,revision,effectiveSource:"definition",
modelRule:"im-next-admission",promptRule:"next-admission",
overrideRule:"instance-explicit",inFlightRule:"pinned"}.
Avatar is empty when absent. revision is an opaque public-projection fingerprint,
not a definition CAS token, monotonic event ID, or Core effective revision.
The header X-IM-Directory-Revision fingerprints the complete directory.
effectiveSource describes this directory's source, NOT an individual user's
runtime selection; explicit overrides still require the authorized Core view.
No prompt, model credentials, private overrides or unknown config are returned.

GET /api/v1/im/agents/events?after=<directory-revision>
returns {code:0,message:"success",data:{revision:"...",changed:true|false}}.
An omitted/unknown cursor means changed=true; an equal cursor means false.
Both endpoints require an enabled authenticated user, not admin permissions,
and use Cache-Control: no-store. Poll at most once per 2s while visible, then
reread the directory on changed=true. Focus/reconnect also rereads. Clients may
get the initial cursor from events if the proxy does not expose the header.
This is current-state invalidation, NOT the privileged durable admin event feed.
It covers public fields, definition update time and directory membership; it
does not claim to fingerprint shared skill/tool changes or per-instance config.
Execution safety never depends on observing an invalidation notification.

## Integration and rollout

Core lifecycle and im-dispatch support must ship with IM, and Core must have
migrated agent_templates.deleted_at. The validation snapshot uses the approved
Shared 1c780d3 tombstone model; production dependency pins remain parent-owned.
Undeployed/denied lifecycle endpoints fail closed (4xx makes pending work terminal).
503/429/transport/malformed responses prevent delivery and remain retryable.
Restored definitions may serve new inbox admissions, never pre-barrier work.
Generic terminal error notifications contain no saved answer/upstream error
string and are still gated by current viewer account/channel membership.
No claim of atomicity across a Core check and an external WuKong HTTP send:
already-sent bytes cannot be recalled. Core owns cancellation and its admission
transaction; IM checks immediately before each delivery boundary.

## Boundaries

Read workspace AGENTS, repo AGENTS, docs/nucleagent/09-workspace-frontend-maintenance.md,
11-test-performance.md, task impl-common.md/c1-common.md, /tmp/api-contract.md,
approved FINAL report and its definition-merged.md attachment, and ponytail skill.
12-dual-remote-sync.md is absent; workspace fallback read. No push/deploy is allowed.
All tests use local fixtures/mocks; no paid calls or shared dependency edits.
