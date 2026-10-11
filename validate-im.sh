#!/usr/bin/env bash
set -euo pipefail
umask 077
root="$(cd "$(dirname "$0")" && pwd)"
workspace=/home/workspace/nucleagent-workspace
shared_revision="$(node -e 'const fs=require("fs"); const lock=JSON.parse(fs.readFileSync(process.argv[1])); const pin=lock.dependencies.find(d=>d.name==="nucleagent-shared")?.commit; if (!/^[0-9a-f]{40}$/.test(pin??"")) process.exit(1); process.stdout.write(pin)' "$root/workspace.lock.json")"
localdir="$root/.validation/shared-$shared_revision"
mkdir -p "$localdir"
cd "$root/app/src/server"
if [[ ! -f "$localdir/ready-tombstone" ]]; then
  for spec in \
    "prism-fusion a32f03b296121ef3fc7f9d257ab3a0935f2008b6" \
    "nucleagent-shared $shared_revision" \
    "nucleagent-storage 18d943b20005b7a351f96e8175e9c3e5ab2079b6" \
    "nucleagent-storage-ndcs 0ff0b4733f13c12d175fbfe8c2ce1a2752d0002c"; do
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
  touch "$localdir/ready-tombstone"
fi
cd "$root/app/src/server"
export GOWORK=off
export GOFLAGS="-mod=mod -modfile=$localdir/im.mod"
unset IM_TEST_MYSQL_DSN IM_TEST_MYSQL_S6 IM_TEST_MYSQL_S6_QUERIES
exec timeout 180 go "$@"
