const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const { test } = require('node:test');
const workflow = fs.readFileSync('.github/workflows/portable-release.yml', 'utf8').replace(/\r\n/g, '\n');
const platforms = ['linux-portable', 'windows-portable', 'macos-portable', 'macos-intel-portable'];
function job(name) {
  const start = workflow.indexOf(`\n  ${name}:\n`);
  assert.ok(start >= 0, `Missing job ${name}`);
  const rest = workflow.slice(start + 1);
  const next = rest.slice(1).search(/\n  [a-z][\w-]*:\n/);
  return next < 0 ? rest : rest.slice(0, next + 1);
}
function condition(name, overrides = {}) {
  const raw = job(name).match(/^    if: \$\{\{ (.*) \}\}$/m)[1];
  const expression = raw.replace(/needs\.([\w-]+)\.result/g, 'needs["$1"].result');
  return vm.runInNewContext(expression, {
    always: () => true,
    startsWith: (text, prefix) => text.startsWith(prefix),
    github: { event_name: 'push', ref_name: 'jvm-sfx-native' },
    inputs: {},
    needs: Object.fromEntries([...platforms, 'regression-gate'].map(p => [p, { result: 'success' }])),
    ...overrides,
  });
}
const manual = { event_name: 'workflow_dispatch', ref_name: 'jvm-sfx-native' };
test('early publication is a manual boolean, default disabled', () => {
  assert.match(workflow, /publish_before_tests:\n(?:        .*\n)*?        type: boolean\n(?:        .*\n)*?        default: false/);
  assert.ok(condition('publish-release-assets'));
  for (const result of ['failure', 'cancelled', 'skipped']) {
    const needs = Object.fromEntries([...platforms, 'regression-gate'].map(p => [p, { result: 'success' }]));
    needs['regression-gate'].result = result;
    assert.equal(condition('publish-release-assets', { needs }), false);
    assert.equal(condition('publish-release-assets', { needs, github: manual, inputs: { publish_before_tests: false } }), false);
    assert.equal(condition('publish-release-assets', { needs, github: manual, inputs: { publish_before_tests: true } }), true);
    assert.equal(condition('publish-release-assets', { needs, inputs: { publish_before_tests: true } }), false);
  }
});
test('no override bypasses a failed, skipped or cancelled platform build', () => {
  for (const platform of platforms) {
    assert.doesNotMatch(job(platform), /^    continue-on-error: true$/m);
    for (const result of ['failure', 'cancelled', 'skipped']) {
      const needs = Object.fromEntries([...platforms, 'regression-gate'].map(p => [p, { result: 'success' }]));
      needs[platform].result = result;
      assert.equal(condition('publish-release-assets', { needs, github: manual, inputs: { publish_before_tests: true } }), false);
    }
  }
});
test('tests still run with early publication and ordinary publication waits for them', () => {
  assert.match(job('publish-release-assets'), /      - regression-gate\n/);
  for (const early of [false, true]) {
    const context = { github: manual, inputs: { publish_before_tests: early } };
    assert.equal(condition('regression-gate', context), !early);
    assert.equal(condition('regression-after-early-publication', context), early);
  }
});
test('artifact-only and fix branch safeguards survive early publication', () => {
  assert.equal(condition('publish-release-assets', { github: manual, inputs: { publish_before_tests: true, publish_target: 'artifacts-only' } }), false);
  assert.equal(condition('publish-release-assets', { github: { event_name: 'push', ref_name: 'fix/example' } }), false);
});

test('push and manual jvm-sfx-native releases preserve existing tags and assets', () => {
  const publish = job('publish-release-assets');
  const branch = publish.match(/if \[\[ "\$\{GITHUB_REF_NAME\}" == "jvm-sfx-native" &&([\s\S]+?)\n +elif/)?.[1];
  assert.ok(branch, 'Missing run-specific branch tag selection');
  assert.match(branch, /"\$\{GITHUB_EVENT_NAME\}" == "push" \|\| "\$\{GITHUB_EVENT_NAME\}" == "workflow_dispatch"/);
  assert.match(branch, /^ +release_tag="jvm-sfx-native-\$\{version\}"\n +versioned_branch_release="true"$/m);
  assert.match(branch, /"\$\{PUBLISH_TARGET\}" != "release-tag"/);
  assert.match(branch, /-n "\$\{SELECTED_RELEASE_TAG\}" && "\$\{SELECTED_RELEASE_TAG\}" != "\$\{release_tag\}"/);
  assert.match(branch, /exit 1/);
  assert.match(publish, /if \[\[ "\$\{versioned_branch_release\}" != "true" &&\s+\( "\$\{release_tag\}" == "latest-jvm-sfx-native" \|\| "\$\{release_tag\}" == jvm-sfx-native-\* \) \]\]; then/);
  const ensure = publish.match(/      - name: Ensure the versioned jvm-sfx-native tag points to the build\n([\s\S]+?)(?=\n      - name: Publish versioned jvm-sfx-native prerelease)/)?.[1];
  assert.ok(ensure, 'Verify the exact tag before handing publication to the release action');
  assert.match(ensure, /steps\.collect-assets\.outputs\.versioned_branch_release == 'true'/);
  assert.match(ensure, /await ensureVersionedTarget\(/);
  const versioned = publish.match(/      - name: Publish versioned jvm-sfx-native prerelease\n([\s\S]+?)(?=\n      - name:)/)?.[1];
  assert.ok(versioned, 'Missing versioned branch publication');
  assert.match(versioned, /steps\.collect-assets\.outputs\.versioned_branch_release == 'true'/);
  assert.match(versioned, /tag_name: \$\{\{ steps\.collect-assets\.outputs\.release_tag \}\}/);
  assert.doesNotMatch(versioned, /target_commitish:/);
  assert.match(versioned, /^          overwrite_files: false$/m);
  const verify = publish.match(/      - name: Verify versioned jvm-sfx-native assets\n([\s\S]+?)(?=\n      - name:)/)?.[1];
  assert.ok(verify, 'Missing verification of published tag and assets');
  assert.match(verify, /await verifyVersionedTarget\(/);
  assert.match(verify, /await verifyAssets\(.*exact: true/);
  const prune = publish.match(/      - name: Verify rolling upload and prune obsolete build assets\n([\s\S]+?)(?=\n      - name:)/)?.[1];
  assert.match(prune, /steps\.collect-assets\.outputs\.rolling_release == 'true'/);
});

test('Windows qualification loads native DLLs from each installed package', () => {
  const windows = job('windows-portable');
  const gate = windows.match(/      - name: Exercise both Windows packages with a redirected runtime\n([\s\S]+?)(?=\n      - name:)/)?.[1];
  assert.ok(gate, 'Missing installed Windows package qualification');
  assert.match(gate, /Remove-Item Env:HICT_JHDF5_NATIVES_ARCHIVE/);
  assert.match(gate, /Using installed JHDF5 native library/);
  assert.match(gate, /Expand-Archive -LiteralPath \$zip -DestinationPath \$zipRoot\n +\$launcher = Get-ChildItem -LiteralPath \$zipRoot/);
  assert.match(gate, /check_windows_standard_user\.ps1 .* -ZipLauncher \$launcher\.FullName/);
});

test('native cache source hashes exclude generated Python bytecode', () => {
  const sequence = fs.readFileSync('.github/workflows/sequence-tools.yml', 'utf8');
  assert.match(sequence, /'scripts\/sequence_analysis\/\*\*'/);
  for (const [name, text] of [['portable', workflow], ['sequence', sequence]]) {
    for (const match of text.matchAll(/hashFiles\(([^)]+)\)/g)) {
      if (!/['"](?:scripts\/(?:ci|toolchains|sequence_analysis)|tests\/toolchains)\/\*\*/.test(match[1])) continue;
      for (const exclusion of ['!**/__pycache__/**', '!**/*.pyc', '!**/*.pyo'])
        assert.ok(match[1].includes(`'${exclusion}'`), `${name}: generated bytecode must not salt cache source fingerprints`);
    }
  }
});

test('BLAST and SeqKit/ModDotPlot caches track their own source inputs', () => {
  const sequence = fs.readFileSync('.github/workflows/sequence-tools.yml', 'utf8');
  const selector = sequence.match(/\$\{\{\s*(matrix\.component == 'blast' && hashFiles\([\s\S]*?\))\s*\}\}/);
  assert.ok(selector, 'Missing component-specific cache key');
  const componentKey = component => vm.runInNewContext(selector[1], {
    matrix: { component },
    hashFiles: (...paths) => paths.join('\n'),
    format: (template, value) => template.replace('{0}', value),
  });
  const blast = componentKey('blast');
  const seqkitAndModDotPlot = componentKey('sequence');
  assert.match(blast, /scripts\/toolchains\/\*blast\*/);
  assert.match(blast, /third_party\/blast\/\*\*/);
  assert.doesNotMatch(blast, /sequence_analysis|moddotplot|seqkit|go-1\.25\.5/);
  assert.match(seqkitAndModDotPlot, /scripts\/sequence_analysis\/\*\*/);
  assert.match(seqkitAndModDotPlot, /build_sequence_python_linux\.sh/);
  assert.match(seqkitAndModDotPlot, /third_party\/seqkit\/\*\*/);
  assert.match(seqkitAndModDotPlot, /third_party\/moddotplot\/\*\*/);
  assert.match(seqkitAndModDotPlot, /^go-1\.25\.5-/);
  assert.doesNotMatch(seqkitAndModDotPlot, /blast/);
});

test('compiled caches track platform build inputs independently of package transfer', () => {
  for (const platform of platforms) {
    const packaging = job(platform);
    const processing = packaging.match(/^          key: hict-native-v\d+-.*$/m)?.[0];
    const toolchain = packaging.match(/^          key: toolchain-v\d+-.*$/m)?.[0];
    assert.ok(processing && toolchain, `${platform}: missing compiled cache key`);
    assert.match(processing, /'src\/main\/native\/\*\*'/);
    assert.match(processing, /HICT_NATIVE_OPENMP/);
    assert.match(toolchain, /scripts\/toolchains\/build_/);
    assert.match(toolchain, /steps\.tool-sources\.outputs\.sources/);
    for (const key of [processing, toolchain]) {
      assert.match(key, /steps\.runner-image\.outputs\.identity/);
      assert.doesNotMatch(key, /\.github\/workflows\/portable-release\.yml/);
    }
  }
});

test('Linux Conan cache reuse requires an exact source-built ABI scope', () => {
  const linux = job('linux-portable');
  assert.match(linux, /key: hictk-source-conan-v3-linux-x86_64-.*HICT_LINUX_ABI_MODE.*HICTK_REF.*steps\.tool-sources\.outputs\.sources/);
  const step = linux.match(/      - name: Build hictk in Docker\n([\s\S]*?)(?=\n      - name:)/)[1];
  const expression = step.match(/HICTK_CONAN_BUILD_POLICY: \$\{\{ (.*?) \}\}/)[1];
  for (const hit of ['true', 'false', '', undefined]) {
    const actual = vm.runInNewContext(expression.replace("steps.cache-hictk-conan.outputs.cache-hit", 'cacheHit'), { cacheHit: hit });
    assert.equal(actual, hit === 'true' ? 'missing' : '*');
  }
  assert.match(linux, /run: scripts\/ci\/validate_linux_component.sh hictk/);
});

test('source regression gate starts alongside packaging and publication still waits', () => {
  assert.doesNotMatch(job('regression-gate'), /^    needs:/m);
  assert.match(job('publish-release-assets'), /      - regression-gate/);
  for (const platform of platforms)
    assert.ok(job('publish-release-assets').includes(`      - ${platform}`));
  const regression = fs.readFileSync('.github/workflows/java-regressions.yml', 'utf8');
  assert.doesNotMatch(regression, /actions\/download-artifact/, 'Parallel source checks must not consume unfinished package artifacts');
});

test('all portable packages and release provenance share one immutable WebUI revision', () => {
  const resolver = job('resolve-webui');
  assert.match(resolver, /commit: \$\{\{ steps\.pin\.outputs\.commit \}\}/);
  assert.match(resolver, /scripts\/portable\/checkout_webui\.mjs/);
  assert.match(resolver, /git -C "\$\{checkout\}" rev-parse HEAD/);
  for (const platform of platforms) {
    const packaging = job(platform);
    assert.match(packaging, /needs: \[resolve-webui, sequence-tools-[\w-]+\]/);
    assert.match(packaging, /HICT_WEBUI_REF: \$\{\{ needs\.resolve-webui\.outputs\.commit \}\}/);
  }
  const publish = job('publish-release-assets');
  assert.match(publish, /^      - resolve-webui$/m);
  assert.match(publish, /HICT_WEBUI_REF: \$\{\{ needs\.resolve-webui\.outputs\.commit \}\}/);
});
