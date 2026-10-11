# Container Dependencies

The root Dockerfile builds from this repository, not its parent workspace.
Run `docker build -t nucleagent-im .` from the repository root.

`deps/` contains only the non-test package files that `go list -deps`
resolves for the server binary from the four local replacements, plus each
module's original `go.mod`/`go.sum` (and Prism's MIT `LICENSE`).
`dependencies.json` records the exact input commits; they must equal
`workspace.lock.json`. The tracked `go.work` (workspace mode) is kept.

Regenerate after committing server dependency changes:

```sh
node build/prepare-dependencies.mjs /path/to/workspace
```

The script archives committed source; it never pulls, switches, or changes
the sibling repositories. Do not edit snapshots by hand. Development builds
continue using `workspace.lock.json` and the existing sibling checkouts.
