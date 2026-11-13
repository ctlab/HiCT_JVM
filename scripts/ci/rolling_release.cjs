// Used by actions/github-script. Preparing a release is deliberately read-only.
const fs = require('node:fs');
const path = require('node:path');
const { createHash } = require('node:crypto');

async function prepare({ github, context, core, tag }) {
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
    .filter(asset => /^(HiCT-|hict_server-)\d/.test(asset.name)
      && asset.uploader?.login === 'github-actions[bot]')
    .map(({ id, name }) => ({ id, name }))));
}

async function prune({ github, context, tag, releaseId, previousAssets, directory }) {
  const expected = fs.readdirSync(directory, { withFileTypes: true })
    .filter(entry => entry.isFile())
    .map(entry => ({ name: entry.name, size: fs.statSync(path.join(directory, entry.name)).size }));
  if (!expected.length) throw new Error('Refusing cleanup without a local asset set');
  const release = (await github.rest.repos.getReleaseByTag({ ...context.repo, tag })).data;
  if (releaseId && String(release.id) !== String(releaseId)) {
    throw new Error('Rolling release changed during publication; refusing cleanup');
  }
  const current = await github.paginate(github.rest.repos.listReleaseAssets, {
    ...context.repo, release_id: release.id, per_page: 100,
  });
  // Validate the entire new set before the first deletion, even if the uploader
  // has reported success. Incomplete or unexpected uploads preserve old assets.
  for (const file of expected) {
    const uploaded = current.find(asset => asset.name === file.name
      && asset.state === 'uploaded' && asset.size === file.size);
    if (!uploaded) {
      throw new Error(`Uploaded asset missing or incomplete: ${file.name}; refusing cleanup`);
    }
    const hash = createHash('sha256');
    for await (const chunk of fs.createReadStream(path.join(directory, file.name))) hash.update(chunk);
    if (uploaded.digest !== `sha256:${hash.digest('hex')}`) {
      throw new Error(`Uploaded asset digest differs: ${file.name}; refusing cleanup`);
    }
  }
  const names = new Set(expected.map(file => file.name));
  for (const old of previousAssets) {
    if (!names.has(old.name) && current.some(asset => asset.id === old.id && asset.name === old.name)) {
      await github.rest.repos.deleteReleaseAsset({ ...context.repo, asset_id: old.id });
    }
  }
}

module.exports = { prepare, prune };
