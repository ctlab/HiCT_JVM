import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { test } from 'node:test';
import { fileURLToPath } from 'node:url';
import { checkoutWebUI } from './checkout_webui.mjs';

const root = fs.mkdtempSync(path.join(os.tmpdir(), 'hict-checkout-test-'));
const repo = path.join(root, 'upstream');fs.mkdirSync(repo);
function git(...args) {
  const r = spawnSync('git', ['-C', repo, '-c', 'user.name=HiCT test', '-c', 'user.email=test@example.invalid', ...args], { encoding: 'utf8' });
  assert.equal(r.status, 0, r.stderr);return r.stdout.trim();
}
git('init','--quiet','-b','master');
fs.writeFileSync(path.join(repo,'package.json'), '{"version":"1"}');
git('add','package.json');git('commit','--quiet','-m','first');
const first = git('rev-parse','HEAD');
git('tag','-a','v1','-m','annotated tag');
fs.writeFileSync(path.join(repo,'package.json'), '{"version":"2"}');
git('commit','--quiet','-am','second');
const second = git('rev-parse','HEAD');
const destination = path.join(root,'checkouts');fs.mkdirSync(destination);
fs.writeFileSync(path.join(destination,'keep.txt'),'existing user output');

test('exact historical commit, branch and annotated tag select the requested tree', () => {
  for (const [ref, expected] of [[first, first], ['master', second], ['v1', first]]) {
    const result = checkoutWebUI({ repo, ref, root: destination });
    assert.equal(result.commit, expected);
    assert.equal(JSON.parse(fs.readFileSync(result.checkout+'.provenance.json')).commit, expected);
    assert.equal(JSON.parse(fs.readFileSync(path.join(result.checkout,'package.json'))).version, expected===first?'1':'2');
  }
  assert.equal(fs.readFileSync(path.join(destination,'keep.txt'),'utf8'),'existing user output');
});
test('an invalid explicit pin never falls back; only automatic mode may do so', () => {
  assert.throws(()=>checkoutWebUI({ repo, ref:'missing', root:destination }), /No fallback was used/);
  const result=checkoutWebUI({repo,ref:'missing',root:destination,fallback:'master'});
  assert.equal(result.commit,second);assert.equal(result.selectedRef,'master');
  assert.throws(()=>checkoutWebUI({repo,ref:'--upload-pack=bad',root:destination}), /non-option ref/);
});
test('CLI emits only the checkout path on stdout, including paths containing spaces', () => {
  const r=spawnSync(process.execPath,[fileURLToPath(new URL('./checkout_webui.mjs',import.meta.url)),'--repo',repo,'--ref',first,'--root',path.join(root,'space in path')],{encoding:'utf8'});
  assert.equal(r.status,0,r.stderr);
  const checkout=r.stdout.trim();assert.ok(fs.existsSync(path.join(checkout,'package.json')));
  assert.equal(r.stdout.trim().split('\n').length,1);
});
console.log(`Retained checkout fixtures: ${root}`);

test('native payload wrappers honor an explicit pin even beside a sibling checkout', () => {
  const scriptDir = path.dirname(fileURLToPath(import.meta.url));
  const project = path.join(root, 'workspace', 'HiCT_JVM');fs.mkdirSync(project,{recursive:true});
  const sibling = path.join(root, 'workspace', 'HiCT_WebUI');fs.mkdirSync(sibling);
  fs.writeFileSync(path.join(sibling,'package.json'),'{"version":"private-local"}');
  const env = { ...process.env, TEST_SCRIPT_DIR:scriptDir, TEST_PROJECT:project,
    TEST_REPO:repo, TEST_REF:first, TEST_WORK:path.join(root,'wrapper outputs') };
  const scripts = process.platform === 'win32'
    ? ['prepare_tauri_browser_payload_windows.ps1','prepare_electron_browser_payload_windows.ps1']
    : ['prepare_tauri_browser_payload_linux.sh','prepare_electron_browser_payload_linux.sh','prepare_tauri_browser_payload_darwin.sh'];
  for (const name of scripts) {
    const source=fs.readFileSync(path.join(scriptDir,name),'utf8');
    let command,args;
    if (name.endsWith('.ps1')) {
      const fn=source.slice(source.indexOf('function Resolve-WebUiSource {')).split('\n}')[0]+'\n}';
      command='pwsh';args=['-NoProfile','-Command',
        '$ErrorActionPreference="Stop"; $scriptDir=$env:TEST_SCRIPT_DIR; $projectDir=$env:TEST_PROJECT; $WebUiDir=""; $WebUiRef=$env:TEST_REF; $WebUiRepoUrl=$env:TEST_REPO; $WorkRoot=$env:TEST_WORK;\n'+fn+'\nResolve-WebUiSource'];
    } else {
      const fn=source.slice(source.indexOf('resolve_webui_source() {')).split('\n}')[0]+'\n}';
      command='bash';args=['-c','set -euo pipefail\nSCRIPT_DIR="$TEST_SCRIPT_DIR"\nPROJECT_DIR="$TEST_PROJECT"\nWEBUI_REF="$TEST_REF"\nWEBUI_REPO_URL="$TEST_REPO"\nWORK_ROOT="$TEST_WORK"\nunset HICT_WEBUI_DIR\n'+fn+'\nresolve_webui_source'];
    }
    const r=spawnSync(command,args,{encoding:'utf8',env});
    assert.equal(r.status,0,`${name}: ${r.stderr}`);
    const resolved=r.stdout.trim();
    assert.notEqual(resolved,sibling);
    assert.equal(JSON.parse(fs.readFileSync(resolved+'.provenance.json')).commit,first);
  }
  assert.equal(JSON.parse(fs.readFileSync(path.join(sibling,'package.json'))).version,'private-local');
});
