/** Resolve branches, tags and full commit IDs without silently changing a pin.
 * Each checkout has a new directory; existing worktrees are never removed.
 */
import fs from 'node:fs';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { pathToFileURL } from 'node:url';

export function checkoutWebUI({ repo, ref, root, fallback }) {
  if (!repo || !ref || !root || ref.startsWith('-') || fallback?.startsWith('-'))
    throw new Error('Expected repository, non-option ref and destination root.');
  fs.mkdirSync(root, { recursive: true });
  const checkout = fs.mkdtempSync(path.join(path.resolve(root), 'HiCT_WebUI-'));
  const git = (args, allowFailure = false) => {
    const result = spawnSync('git', ['-C', checkout, ...args], { encoding: 'utf8' });
    if (result.error) throw result.error;
    if (result.stderr) process.stderr.write(result.stderr);
    if (result.status !== 0 && !allowFailure)
      throw new Error(`WebUI git ${args[0]} failed. Checkout retained at ${checkout}.`);
    return result;
  };
  git(['init', '--quiet']);
  git(['remote', 'add', 'origin', repo]);
  let selected = ref;
  if (git(['fetch', '--depth=1', 'origin', ref], true).status !== 0) {
    if (!fallback || fallback === ref)
      throw new Error(`Cannot fetch requested WebUI ref '${ref}'. No fallback was used. Checkout retained at ${checkout}.`);
    process.stderr.write(`Automatic WebUI ref '${ref}' unavailable; using '${fallback}'.\n`);
    selected = fallback;
    git(['fetch', '--depth=1', 'origin', selected]);
  }
  git(['checkout', '--quiet', '--detach', 'FETCH_HEAD']);
  const commit = git(['rev-parse', 'HEAD']).stdout.trim();
  if (/^[a-f0-9]{40}$/i.test(selected) && commit.toLowerCase() !== selected.toLowerCase())
    throw new Error(`WebUI commit does not match requested pin '${selected}'.`);
  if (!fs.existsSync(path.join(checkout, 'package.json')))
    throw new Error(`Requested WebUI revision has no package.json: ${commit}`);
  const provenance = { repository: repo, requestedRef: ref, selectedRef: selected, commit, checkout };
  fs.writeFileSync(`${checkout}.provenance.json`, JSON.stringify(provenance, null, 2) + '\n');
  process.stderr.write(`Using WebUI commit ${commit} (requested '${ref}').\n`);
  return provenance;
}

if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
  try {
    const options = {};
    for (let i = 2; i < process.argv.length; i += 2) {
      const key = process.argv[i].replace(/^--/, '');
      if (!['repo', 'ref', 'root', 'fallback'].includes(key) || !process.argv[i + 1])
        throw new Error('Use --repo URL --ref REF --root DIRECTORY [--fallback REF].');
      options[key] = process.argv[i + 1];
    }
    console.log(checkoutWebUI(options).checkout);
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}
