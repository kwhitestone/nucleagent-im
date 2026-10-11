import assert from "node:assert/strict";
import { cpSync, existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, relative, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";
import { execFileSync } from "node:child_process";

// Same as nucleagent-core build/prepare-dependencies.mjs (Auth 3d1f591 mechanism).
// Copies exactly the non-test package files that `go list -deps` resolves
// for the server binary, plus module go.mod/go.sum.
const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const workspace = process.argv[2];
assert.ok(workspace, "Usage: node build/prepare-dependencies.mjs <workspace>");
const dependencies = JSON.parse(readFileSync(join(root, "build/dependencies.json")));
const lock = JSON.parse(readFileSync(join(root, "workspace.lock.json")));
for (const pinned of lock.dependencies.filter(d => d.goModules?.length)) {
  assert.equal(dependencies.find(d => d.name === pinned.name)?.commit, pinned.commit, pinned.name);
}
const scratch = mkdtempSync(join(tmpdir(), "im-dependencies-"));
const run = (command, args, options = {}) =>
  execFileSync(command, args, { timeout: 300000, maxBuffer: 256 * 1024 * 1024, ...options });
function archive(repo, commit, destination, paths = []) {
  mkdirSync(destination, { recursive: true });
  run("tar", ["-x", "-C", destination], {
    input: run("git", ["-C", repo, "archive", commit, ...paths]),
  });
}
try {
  // Archive committed inputs so unrelated worktree edits cannot enter the snapshot.
  archive(root, "HEAD", join(scratch, "nucleagent-im"), ["app/src/server"]);
  for (const dependency of dependencies) {
    assert.match(dependency.commit, /^[a-f0-9]{40}$/);
    archive(join(workspace, dependency.name), dependency.commit, join(scratch, dependency.name));
  }
  const server = join(scratch, "nucleagent-im/app/src/server");
  const listed = run("go", ["list", "-deps", "-f",
    "{{.Dir}}\t{{join .GoFiles \",\"}}\t{{join .EmbedFiles \",\"}}\t{{join .SFiles \",\"}}", "."],
    { cwd: server, encoding: "utf8", env: { ...process.env, CGO_ENABLED: "0" } });
  const output = join(root, "build/deps");
  rmSync(output, { recursive: true, force: true });
  const used = new Set();
  for (const line of listed.trim().split("\n")) {
    const [dir, ...groups] = line.split("\t");
    const rel = relative(scratch, dir);
    if (rel.startsWith("..") || rel.startsWith("nucleagent-im" + sep)) continue;
    const name = rel.split(sep)[0];
    assert.ok(dependencies.some(d => d.name === name), `unpinned sibling ${name}`);
    used.add(name);
    for (const file of groups.flatMap(g => g ? g.split(",") : [])) {
      assert.ok(!file.endsWith("_test.go"));
      cpSync(join(dir, file), join(output, rel, file));
    }
  }
  for (const dependency of dependencies) {
    assert.ok(used.has(dependency.name), `${dependency.name} pinned but unused`);
    const moduleRoot = join(dependency.name, dependency.path);
    for (const file of ["go.mod", "go.sum"]) {
      if (existsSync(join(scratch, moduleRoot, file))) cpSync(join(scratch, moduleRoot, file), join(output, moduleRoot, file));
    }
    if (dependency.license) cpSync(join(scratch, dependency.name, dependency.license), join(output, dependency.name, dependency.license));
  }
  console.log(`Prepared ${used.size} pinned module snapshots.`);
} finally {
  rmSync(scratch, { recursive: true, force: true });
}
