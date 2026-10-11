#!/usr/bin/env bash
set -euo pipefail
umask 077
root="$(cd "$(dirname "$0")" && pwd)"
workspace=/home/workspace/nucleagent-workspace
localdir="$root/.validation"
mkdir -p "$localdir"
cd "$root/app/src/server"
if [[ ! -f "$localdir/ready" ]]; then
  for spec in \
    "prism-fusion a32f03b296121ef3fc7f9d257ab3a0935f2008b6" \
    "nucleagent-shared 3eea90f36e842752f6a7606ecebfd1e6daa16947" \
    "nucleagent-storage HEAD" \
    "nucleagent-storage-ndcs HEAD"; do
    read -r name revision <<< "$spec"
    mkdir -p "$localdir/$name"
    git -C "$workspace/$name" archive "$revision" | tar -x -C "$localdir/$name"
  done
  cp "$root/app/src/server/go.mod" "$localdir/im.mod"
  cp "$root/app/src/server/go.sum" "$localdir/im.sum"
  GOWORK=off go mod edit -modfile="$localdir/im.mod" \
    -replace="github.com/kwhitestone/prism-fusion=$localdir/prism-fusion/src/server" \
    -replace="github.com/nucleagent/nucleagent-shared=$localdir/nucleagent-shared" \
    -replace="github.com/kwhitestone/nucleagent-storage=$localdir/nucleagent-storage/app/src/server" \
    -replace="github.com/kwhitestone/nucleagent-storage-ndcs=$localdir/nucleagent-storage-ndcs"
  touch "$localdir/ready"
fi
cd "$root/app/src/server"
export GOWORK=off
export GOFLAGS="-mod=mod -modfile=$localdir/im.mod"
exec timeout 180 go "$@"
