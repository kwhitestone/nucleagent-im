# IM backend delivery report

- Scope: feat/im-agent-impl-backend only, base a82f9c7; no Core/Shared/Auth/frontend/search edits.
- Commits: 5477e8d (red tests), 7a2c6fb (implementation); this report is a separate commit.
- Contract: IM-LIFECYCLE-NOTES.md; checked against /tmp/api-contract.md v2.
- Production files (under app/src/server/addons/im):
  agents.go:64,127 - fresh tombstone/Auth-aware directory, avatar/public revision/source, nonadmin invalidation.
  lifecycle.go:20,38 - authoritative Core checks, immutable admission, terminal denial, post-request local rechecks.
  recipients.go:54 - current account/type, group membership and invocation permission checks.
  worker.go:216,372 - dispatch admission timestamp and final-delivery lifecycle guards.
  stream.go:27,259 - Core snapshot and browser delivery/viewer rechecks; sanitized terminal notifications.
- Test files: agents_test.go, lifecycle_test.go, dispatch_correlation_test.go,
  m2_test.go, m3_a2a_test.go, stream_replay_test.go (same directory).
- Support files: .gitignore, validate-im.sh, IM-LIFECYCLE-NOTES.md, IM-BACKEND-REPORT.md.
- No IM definition copy or directory cache; public tokens are not CAS or runtime effective revisions.
- TDD: initial lifecycle suite red -> green; additional tombstone, public metadata and revocation races red -> green.
- Full suite: passing entries 152 -> 189 (includes subtests); top-level passes 101 -> 110; skips 4 -> 4.
- Full JSON event span: 3.261s -> 1.930s; IM package elapsed: 2.103s -> 1.930s.
- Focused suite: 11 -> 48 passing entries, 0.939s -> 0.522s (new lifecycle cases included).
- Five mutations detected: removed tombstone filter, final guard, per-snapshot guard,
  group invocation denial, post-Core viewer recheck. All expected failures; all restored.
- Focused race: 38 passing entries, 1.815s. go vet ./... passed.
- Build: ordinary build failed isolated-archive VCS stamping; build -buildvcs=false ./... passed.
- Reproduce: bash validate-im.sh test -json -count=1 -p 2 ./...
- Fast: bash validate-im.sh test -json -count=1 ./addons/im -run 'TestAgentDirectory|TestQueuedRecipient|TestCoreStream|TestDispatch|TestAgentStream|TestFinalWuKong|TestLifecycle'
- Race: bash validate-im.sh test -race -count=1 ./addons/im -run 'TestAgentDirectory|TestLifecycle'
- Final runs used GOMAXPROCS=2; isolated pinned dependencies in ignored .validation/.
- Evidence: .validation/{baseline,full-baseline,full-final,focused-final,race-final}.json;
  mutation-{1-tombstone,2-final,3-stream,4-group,5-viewer}.json; vet/build logs. Evidence mode 0600.
- Four existing MySQL search integration cases skipped; local SQLite/mocks only, no live DB/model/browser E2E calls.
- No push, rebase, main movement, deployment, other process/page/port changes, or outside-worktree writes.
- Parent integration required: implement POST /api/v1/addons/conversation/im-lifecycle with raw JSON;
  durable disable/delete barrier across restore, plus atomic admittedAt enforcement in im-dispatch.
- Rollout requires Core tombstone migration and compatible Shared dependency; parent owns production pins.
- Missing lifecycle endpoint fails closed; its 404 terminalizes pending work, so Core and IM must ship together.
- Core owns cancellation and resource/runtime pin enforcement. External delivery is not transactional with Core.
- Public events are polling fingerprints only; no admin event detail, prompt/config or private overrides.
- Unverified: live cross-service integration, deployment/rollback and real-model end-to-end behavior.
