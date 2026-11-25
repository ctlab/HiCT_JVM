// Used by actions/github-script. Preparing a release is deliberately read-only.
const fs = require('node:fs');
const path = require('node:path');
const { createHash } = require('node:crypto');

function isManagedAsset(asset) {
  return asset.uploader?.login === 'github-actions[bot]'
    && /^(HiCT-|hict_server-)\d/.test(asset.name);
}

function requireRollingTag(tag) {
  if (tag === 'latest-jvm-sfx-native' || tag.startsWith('jvm-sfx-native-')) {
    throw new Error(`Rolling publication is disabled for ${tag}`);
  }
}

async function prepare({ github, context, core, tag }) {
  requireRollingTag(tag);
  let release;
  try {
    release = (await github.rest.repos.getReleaseByTag({ ...context.repo, tag })).data;
  } catch (error) {
    if (error.status !== 404) throw error;
  }
  if (release?.immutable) throw new Error(`Rolling release ${tag} is immutable`);
  const branch = context.payload.repository.default_branch;
  if (!branch) throw new Error('Missing repository default branch');
  const assets = release
    ? await github.paginate(github.rest.repos.listReleaseAssets, {
        ...context.repo, release_id: release.id, per_page: 100,
      })
    : [];
  // Re-sending an old target whose workflows differ from the default branch
  // requires Workflows:write, which GITHUB_TOKEN cannot obtain. For an existing
  // tag this field does not move the tag. A new tag still targets the build SHA.
  core.setOutput('target_commitish', release ? branch : context.sha);
  core.setOutput('release_id', release?.id || '');
  core.setOutput('previous_assets', JSON.stringify(assets
    .filter(isManagedAsset)
    .map(({ id, name }) => ({ id, name }))));
}

async function verifyVersionedTarget({ github, context, tag, allowMissing = false }) {
  let ref;
  try {
    ({ data: ref } = await github.rest.git.getRef({ ...context.repo, ref: `tags/${tag}` }));
  } catch (error) {
    if (allowMissing && error.status === 404) return;
    throw error;
  }
  if (ref.object.type !== 'commit' || ref.object.sha !== context.sha) {
    throw new Error(`Release tag ${tag} does not point to ${context.sha}`);
  }
}

async function verifyAssets({ github, context, tag, releaseId, directory, exact = false }) {
  const expected = fs.readdirSync(directory, { withFileTypes: true })
    .filter(entry => entry.isFile())
    .map(entry => ({ name: entry.name, size: fs.statSync(path.join(directory, entry.name)).size }));
  if (!expected.length) throw new Error('Cannot verify a release without a local asset set');
  const release = (await github.rest.repos.getReleaseByTag({ ...context.repo, tag })).data;
  if (releaseId && String(release.id) !== String(releaseId)) {
    throw new Error('Rolling release changed during publication; refusing cleanup');
  }
  const current = await github.paginate(github.rest.repos.listReleaseAssets, {
    ...context.repo, release_id: release.id, per_page: 100,
  });
  const names = new Set(expected.map(file => file.name));
  if (exact) {
    const unexpected = current.filter(asset => !names.has(asset.name));
    if (unexpected.length) {
      throw new Error(`Unexpected versioned release assets: ${unexpected.map(asset => asset.name).join(', ')}`);
    }
  }
  for (const file of expected) {
    const uploaded = current.find(asset => asset.name === file.name
      && asset.state === 'uploaded' && asset.size === file.size);
    if (!uploaded) {
      throw new Error(`Uploaded asset missing or incomplete: ${file.name}`);
    }
    const hash = createHash('sha256');
    for await (const chunk of fs.createReadStream(path.join(directory, file.name))) hash.update(chunk);
    if (uploaded.digest !== `sha256:${hash.digest('hex')}`) {
      throw new Error(`Uploaded asset digest differs: ${file.name}`);
    }
  }
  return { current, names };
}

async function prune({ github, context, tag, releaseId, previousAssets, directory }) {
  requireRollingTag(tag);
  // Validate the entire upload before deleting any obsolete assets.
  const { current, names } = await verifyAssets({ github, context, tag, releaseId, directory });
  for (const old of previousAssets) {
    if (!names.has(old.name) && current.some(asset =>
      asset.id === old.id && asset.name === old.name && isManagedAsset(asset))) {
      await github.rest.repos.deleteReleaseAsset({ ...context.repo, asset_id: old.id });
    }
  }
}

module.exports = { prepare, prune, verifyAssets, verifyVersionedTarget };
