const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { spawnSync } = require("node:child_process");
const { test } = require("node:test");
const version = "2.0.49-current-webui_current";
function fixture(t) {
  const root = fs.mkdtempSync(
    path.join(os.tmpdir(), "HiCT package selection ")
  );
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  fs.mkdirSync(path.join(root, "build", "libs"), { recursive: true });
  fs.writeFileSync(path.join(root, "version.txt"), version + "\n");
  for (const [name, content, time] of [
    [`hict_server-${version}-fat.jar`, "CURRENT", 1000],
    ["hict_server-2.0.9-old-fat.jar", "ALPHABETIC_STALE", 2000000000],
    ["hict_server-9.0.0-other-fat.jar", "NEWER_STALE", 2000000100],
  ]) {
    const target = path.join(root, "build", "libs", name);
    fs.writeFileSync(target, content);
    fs.utimesSync(target, time, time);
  }
  return root;
}
for (const script of [
  "build_portable_linux.sh",
  "build_portable_darwin_arm64.sh",
]) {
  const source = fs.readFileSync(`scripts/portable/${script}`, "utf8");
  const begin = source.indexOf("\nFAT_JAR=");
  const end = source.indexOf('\nif ! "${JAR_TOOL}" tf', begin);
  assert(
    begin >= 0 && end > begin,
    "selection must be exercised before payload inspection"
  );
  const block = source.slice(begin, end);
  test(
    `${script} selects the declared version despite stale files and timestamps`,
    { skip: process.platform === "win32" },
    (t) => {
      const root = fixture(t);
      const run = () =>
        spawnSync("bash", ["-eu", "-c", `${block}\ncat "$FAT_JAR"`], {
          encoding: "utf8",
          env: { ...process.env, PROJECT_DIR: root, VERSION: version },
        });
      let result = run();
      assert.equal(result.status, 0, result.stderr);
      assert.equal(result.stdout, "CURRENT");
      fs.unlinkSync(
        path.join(root, "build", "libs", `hict_server-${version}-fat.jar`)
      );
      result = run();
      assert.equal(result.status, 1);
      assert.match(result.stderr, /current version fat JAR was not found/);
      assert(!result.stdout.includes("STALE"));
    }
  );
}
const psAvailable =
  spawnSync(
    "pwsh",
    [
      "-NoLogo",
      "-NoProfile",
      "-NonInteractive",
      "-Command",
      "$PSVersionTable.PSVersion.ToString()",
    ],
    { encoding: "utf8" }
  ).status === 0;
test(
  "Windows packaging selects the declared version and rejects stale fallback",
  { skip: !psAvailable && process.platform !== "win32" },
  (t) => {
    assert(psAvailable, "PowerShell must be available on Windows CI");
    const root = fixture(t),
      source = fs.readFileSync(
        "scripts/portable/build_portable_windows.ps1",
        "utf8"
      );
    const begin = source.indexOf("$fatJarPath ="),
      end = source.indexOf("$jarEntries =", begin);
    assert(begin >= 0 && end > begin);
    const script = `$ErrorActionPreference='Stop';$projectDir=$env:HICT_SELECTION_PROJECT;$version=$env:HICT_SELECTION_VERSION;\n${source.slice(
      begin,
      end
    )}\nGet-Content -LiteralPath $fatJar.FullName -Raw`;
    const run = () =>
      spawnSync(
        "pwsh",
        ["-NoLogo", "-NoProfile", "-NonInteractive", "-Command", script],
        {
          encoding: "utf8",
          env: {
            ...process.env,
            HICT_SELECTION_PROJECT: root,
            HICT_SELECTION_VERSION: version,
          },
        }
      );
    let result = run();
    assert.equal(result.status, 0, result.stderr);
    assert.equal(result.stdout.trim(), "CURRENT");
    fs.unlinkSync(
      path.join(root, "build", "libs", `hict_server-${version}-fat.jar`)
    );
    result = run();
    assert.notEqual(result.status, 0);
    assert.match(result.stderr, /current version fat JAR was not found/);
  }
);
for (const abi of ["glibc217", "musl-static"])
  test(
    `Linux ${abi} validation inspects only the current portable root`,
    { skip: process.platform === "win32" },
    (t) => {
      const root = fixture(t),
        suffix = abi === "musl-static" ? "-musl" : "";
      const expected = path.join(
        root,
        "build",
        "portable",
        `HiCT-${version}${suffix}-linux-x86_64`
      );
      fs.mkdirSync(expected, { recursive: true });
      fs.mkdirSync(
        path.join(root, "build", "portable", "HiCT-9.0.0-linux-x86_64")
      );
      const source = fs.readFileSync(
        "scripts/ci/validate_linux_component.sh",
        "utf8"
      );
      const begin = source.indexOf("\n  portable)") + "\n  portable)".length,
        end = source.indexOf("\n    scan_glibc_floor", begin);
      const run = () =>
        spawnSync(
          "bash",
          ["-eu", "-c", source.slice(begin, end) + '\nprintf "%s" "$root"'],
          {
            cwd: root,
            encoding: "utf8",
            env: { ...process.env, HICT_LINUX_ABI_MODE: abi },
          }
        );
      let result = run();
      assert.equal(result.status, 0, result.stderr);
      assert.equal(result.stdout, path.relative(root, expected));
      fs.rmdirSync(expected);
      result = run();
      assert.equal(result.status, 1);
    }
  );
test("CI uploads and smoke checks use the stamped artifact identity", () => {
  const workflow = fs.readFileSync(
    ".github/workflows/portable-release.yml",
    "utf8"
  );
  const uploads = workflow
    .split("\n")
    .filter((line) =>
      /^\s+(?:path: )?build\/(?:distributions\/HiCT-|libs\/)/.test(line)
    );
  assert(uploads.length >= 15, "Expected all platform upload declarations");
  for (const line of uploads)
    assert(line.includes("${{ env.HICT_BUILD_VERSION }}"), line);
  assert.equal(
    (workflow.match(/set_release_version\.py --github-env/g) || []).length,
    5
  );
  assert.doesNotMatch(
    workflow,
    /find build\/(?:libs|portable|distributions).*\| sort \| tail -n 1/
  );
  assert.doesNotMatch(workflow, /\$exe = Get-ChildItem/);
});
