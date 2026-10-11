# IM backend delivery report

- Scope: feat/im-agent-impl-backend only, base a82f9c7; no Core/Shared/Auth/frontend/search edits.
- Commits: 5477e8d (red), 7a2c6fb (implementation), 090ab66 (merged Shared pin), f69775a (module declarations).
- Final tested code: f69775a6d30d6640be2cc996f54a71863872c362; this report is a documentation-only follow-up.
- Contract: IM-LIFECYCLE-NOTES.md; public directory and lifecycle interfaces retained in API contract v6.
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
- Full IM package elapsed: baseline 2.103s -> prior 1.930s -> final 1.966s; final command wall 21.806s.
- Focused suite: 11 -> 48 passing entries, 0.939s -> 0.522s (new lifecycle cases included).
- Five mutations detected: removed tombstone filter, final guard, per-snapshot guard,
  group invocation denial, post-Core viewer recheck. All expected failures; all restored.
- Final focused race: 38 passing entries, 1.910s (prior 1.815s), 0 failures; wall 35.216s including compilation.
- Final vet/build PASS: wall 9.266s/4.222s. Strict -mod=readonly -buildvcs=false; no module resolution writes.
- Exact archive reproduction: .validation/final-gates.mjs prepare|go|docker at the tested code revision.
- Fast: bash validate-im.sh test -json -count=1 ./addons/im -run 'TestAgentDirectory|TestQueuedRecipient|TestCoreStream|TestDispatch|TestAgentStream|TestFinalWuKong|TestLifecycle'
- Race: bash validate-im.sh test -race -count=1 ./addons/im -run 'TestAgentDirectory|TestLifecycle'
- Final host runs: GOMAXPROCS=2, -p 2, GOWORK=off, GOPROXY=off, GOSUMDB=off; independent git archives.
- Evidence: .validation/{baseline,full-baseline,full-final,focused-final,race-final}.json;
  mutation-{1-tombstone,2-final,3-stream,4-group,5-viewer}.json; vet/build logs. Evidence mode 0600.
- Final evidence root: .validation/final-f69775a6d30d6640be2cc996f54a71863872c362/ (inputs.json, go-results.json, logs).
- Final archive Docker PASS 29.514s; docker-result.json and image-permissions.json preserve receipts.
- Image: sha256:d30a8f2fab6fbe06986857dc4b6dffca56f9b8c6e1d9525c479a952b13e40615; OCI revision equals tested code.
- UID10001/read-only/network-none config readability and executable check PASS; own check container removed.
- Shared ba965e60243aa5c208f90a35f15dd3ab295d434e; Prism a32f03b296121ef3fc7f9d257ab3a0935f2008b6.
- Storage 18d943b20005b7a351f96e8175e9c3e5ab2079b6; NDCS 0ff0b4733f13c12d175fbfe8c2ce1a2752d0002c.
- Preliminary 090ab66 Docker PASS but strict full failed missing go.mod declarations; preserved, not relabeled.
- Four existing MySQL search integration cases skipped; local SQLite/mocks only, no live model or E2E claim.
- No push, rebase, main movement, deployment, paid calls, foreign process/page/port changes.
- Parent reports Core b3fc6ea implements the lifecycle/barrier interface; production integration remains its gate.
- Rollout requires Core tombstone migration and compatible Shared pin; parent owns merge/release.
- Missing lifecycle endpoint fails closed; its 404 terminalizes pending work, so Core and IM must ship together.
- Core owns cancellation and resource/runtime pin enforcement. External delivery is not transactional with Core.
- Public events are polling fingerprints only; no admin event detail, prompt/config or private overrides.
- Unverified: live cross-service integration, deployment/rollback and real-model end-to-end behavior.
