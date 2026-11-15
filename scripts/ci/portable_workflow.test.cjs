const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const { test } = require('node:test');
const workflow = fs.readFileSync('.github/workflows/portable-release.yml', 'utf8');
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
