const assert = require('node:assert/strict');
const { test } = require('node:test');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { createHash } = require('node:crypto');
const { prepare, prune } = require('./rolling_release.cjs');

const context = {
  repo: { owner: 'ctlab', repo: 'HiCT_JVM' },
  sha: 'build-sha',
  payload: { repository: { default_branch: 'master' } },
};
const automated = (id, name, size = 3) => ({
  id, name, size, state: 'uploaded', uploader: { login: 'github-actions[bot]' },
  digest: `sha256:${createHash('sha256').update('jar').digest('hex')}`,
});
const old = automated(1, 'HiCT-2.0.1-linux.run');
const next = automated(2, 'HiCT-2.0.2-linux.run');

function client(assets = [old], release = { id: 42, target_commitish: 'old-june-sha' }) {
  const deleted = [], outputs = {};
  const api = {
    getReleaseByTag: async () => ({ data: release }),
    listReleaseAssets: async () => {},
    deleteReleaseAsset: async ({ asset_id }) => deleted.push(asset_id),
  };
  return {
    github: { rest: { repos: api }, paginate: async (method, args) => {
      assert.equal(method, api.listReleaseAssets);
      assert.equal(args.release_id, release.id);
      assert.equal(args.per_page, 100);
      return assets;
    } },
    core: { setOutput: (name, value) => { outputs[name] = value; } },
    context, tag: 'latest-master', deleted, outputs,
  };
}

test('old rolling release uses the live default branch without moving its tag or deleting assets', async () => {
  const c = client();
  await prepare(c);
  assert.equal(c.outputs.target_commitish, 'master');
  assert.equal(c.outputs.release_id, 42);
  assert.deepEqual(JSON.parse(c.outputs.previous_assets), [{ id: 1, name: old.name }]);
  assert.deepEqual(c.deleted, []);
});

test('first publication creates its tag at the build SHA, not the default branch', async () => {
  const c = client();
  c.github.rest.repos.getReleaseByTag = async () => { throw { status: 404 }; };
  await prepare(c);
  assert.equal(c.outputs.target_commitish, context.sha);
  assert.deepEqual(JSON.parse(c.outputs.previous_assets), []);
  assert.deepEqual(c.deleted, []);
});

test('authorization and transient read failures cannot be mistaken for a missing release', async () => {
  for (const status of [401, 403, 429, 500]) {
    const c = client();
    c.github.rest.repos.getReleaseByTag = async () => { throw { status }; };
    await assert.rejects(prepare(c), error => error.status === status);
    assert.deepEqual(c.outputs, {});
    assert.deepEqual(c.deleted, []);
  }
});

test('immutable releases stop publication before modifying assets', async () => {
  const c = client([old], { id: 42, immutable: true });
  await assert.rejects(prepare(c), /immutable/);
  assert.deepEqual(c.outputs, {});
  assert.deepEqual(c.deleted, []);
});

test('snapshot preserves manual uploads and unrelated assets', async () => {
  const c = client([old, automated(3, 'manual-notes.txt'), {
    ...automated(4, 'HiCT-2.0.1-custom.zip'), uploader: { login: 'maintainer' },
  }, automated(5, 'hict_server-2.0.1-fat.jar')]);
  await prepare(c);
  assert.deepEqual(JSON.parse(c.outputs.previous_assets).map(asset => asset.id), [1, 5]);
});

async function withUpload(assets, callback) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'hict-release-test-'));
  try {
    fs.writeFileSync(path.join(directory, next.name), 'jar');
    const c = { ...client(assets), releaseId: 42, previousAssets: [old], directory };
    await callback(c);
  } finally {
    fs.rmSync(directory, { recursive: true, force: true });
  }
}

test('cleanup preserves old downloads if any new asset is missing, incomplete or has the wrong size', async () => {
  for (const assets of [[old], [old, { ...next, size: 1 }], [old, { ...next, state: 'starter' }]]) {
    await withUpload(assets, async c => {
      await assert.rejects(prune(c), /missing or incomplete/);
      assert.deepEqual(c.deleted, []);
    });
  }
});

test('successful upload removes only obsolete snapshotted automated assets', async () => {
  const concurrent = automated(3, 'HiCT-2.0.3-newer.zip');
  await withUpload([old, next, concurrent], async c => {
    await prune(c);
    assert.deepEqual(c.deleted, [old.id]);
  });
});

test('the branch rolling release prunes previous installer sets and preserves other downloads', async () => {
  const previousVersion = '2.0.409-ci1-7b2608c';
  const suffixes = [
    'linux-x86_64.run', 'musl-linux-x86_64.tar.gz', 'linux-x86_64.sha256',
    'darwin-arm64.run', 'darwin-x86_64.tar.gz', 'darwin-arm64.sha256',
    'windows-x86_64.exe', 'windows-x86_64-portable.zip', 'windows-x86_64.sha256',
    'x86_64.AppImage', 'x86_64.AppImage.sha256', 'build-origin.txt', 'release-assets.sha256',
  ];
  const obsolete = suffixes.map((suffix, i) => automated(100 + i, `HiCT-${previousVersion}-${suffix}`));
  obsolete.push(automated(200, `hict_server-${previousVersion}-fat.jar`));
  const unrelated = [
    automated(201, `HiCT-${previousVersion}-workshop-data.zip`),
    { ...automated(202, 'HiCT-2.0.408-ci1-0123456-linux-x86_64.run'), uploader: { login: 'maintainer' } },
    automated(203, 'HiCT-2.0.0-archive-linux-x86_64.run'),
  ];
  const uploaded = { ...next, name: 'HiCT-2.0.418-ci1-0702614-linux-x86_64.run' };
  const assets = [...obsolete, ...unrelated];
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'hict-release-test-'));
  try {
    const c = { ...client(assets), tag: 'latest-jvm-sfx-native', directory };
    await prepare(c);
    c.releaseId = c.outputs.release_id;
    c.previousAssets = JSON.parse(c.outputs.previous_assets);
    assert.deepEqual(c.previousAssets.map(asset => asset.id), obsolete.map(asset => asset.id));
    assert.deepEqual(c.deleted, []);
    fs.writeFileSync(path.join(directory, uploaded.name), 'jar');
    await assert.rejects(prune(c), /missing or incomplete/);
    assert.deepEqual(c.deleted, []);
    assets.push(uploaded, automated(204, 'HiCT-2.0.419-ci1-0123456-linux-x86_64.run'));
    await prune(c);
    assert.deepEqual(c.deleted, obsolete.map(asset => asset.id));
  } finally {
    fs.rmSync(directory, { recursive: true, force: true });
  }
});

test('same-size content mismatches and missing digests preserve every old asset', async () => {
  for (const digest of [undefined, 'sha256:wrong']) {
    await withUpload([old, { ...next, digest }], async c => {
      await assert.rejects(prune(c), /digest differs/);
      assert.deepEqual(c.deleted, []);
    });
  }
});

test('first publication verifies its uploaded files without deleting anything', async () => {
  await withUpload([next], async c => {
    c.releaseId = '';
    c.previousAssets = [];
    await prune(c);
    assert.deepEqual(c.deleted, []);
  });
});

test('cleanup cannot delete an asset reused by the new set or a replacement with a different ID', async () => {
  await withUpload([old, next], async c => {
    c.previousAssets = [next];
    await prune(c);
    assert.deepEqual(c.deleted, []);
  });
  await withUpload([{ ...old, id: 99 }, next], async c => {
    await prune(c);
    assert.deepEqual(c.deleted, []);
  });
});

test('a replaced release or an empty local set prevents cleanup', async () => {
  await withUpload([old, next], async c => {
    c.releaseId = 99;
    await assert.rejects(prune(c), /changed during publication/);
    assert.deepEqual(c.deleted, []);
  });
  await withUpload([old, next], async c => {
    fs.unlinkSync(path.join(c.directory, next.name));
    await assert.rejects(prune(c), /without a local asset set/);
    assert.deepEqual(c.deleted, []);
  });
});
