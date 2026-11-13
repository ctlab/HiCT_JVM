# Rolling release publication

## Master publication failure, 2026-11-13

[Run 43478127219](https://github.com/ctlab/HiCT_JVM/actions/runs/43478127219),
commit `fc2d13e234e1ab5ecbb6fae2185424ab397a564a`, built all four platform
packages successfully. Only Publish release assets failed. Its token had
`contents: write`; adding that permission again cannot fix this failure.

The action found release 324497952 (`latest-master`) and then tried to update
its metadata. Its error handler calls update failures an error fetching the
release and prints the triggering ref (`refs/heads/master`), even though it
successfully selected `latest-master`.

The release's stored `target_commitish` was the June commit
`25ebabc71180048a3365b37fe89c1442e853e404`. All four workflow files have changed
since that target. `softprops/action-gh-release` resends this target when no
override is supplied. GitHub's release-update permission check requires workflow
write authorization when the resolved target's workflows differ from the default
branch. `GITHUB_TOKEN` cannot obtain that authorization. This evidence matches
the 403; no mutating API probe was made against the production release.

References: [GitHub release-update permissions](https://docs.github.com/en/rest/releases/releases#update-a-release)
and [the action's existing-release update](https://github.com/softprops/action-gh-release/blob/3bb12739c298aeb8a4eeaf626c5b8d85266b0e65/src/github.ts).

The old job cleared the release's assets before its metadata update failed.
At inspection on 2026-11-14 the release had zero assets. The failed run's packaged
Actions artifacts were still present and unexpired. Existing published downloads
were not deleted or altered during this investigation.

## Corrected sequence

1. Read the existing release and snapshot only automated HiCT assets. Only a
   genuine 404 means a release is absent; authorization/network errors stop here.
2. For an existing rolling release, supply the live default branch as the metadata
   target. GitHub does not move an existing tag when this field is supplied.
   A new release still uses the exact build SHA to create its tag. An immutable
   existing release is rejected before publication.
3. Update the rolling release and upload without overwriting same-name assets.
   A failed metadata update or upload cannot remove the previous files.
4. Verify every local file against the uploaded name, state, size and SHA-256
   digest. Only then delete obsolete automated assets from the original snapshot.
   Retain the new files, manual uploads and assets added after that snapshot.
   If the release ID changed during publication, stop without cleanup.

This requires no PAT or additional token permission for the existing master
rolling release. Creating a new tag at a non-default commit whose workflows
differ can still require owner-authorized workflow permissions; a denied creation
stops without pruning. Immutable releases and repository policies remain enforced.

The tag and artifact provenance are distinct: rolling tags are not force-moved;
`build-origin.txt`, checksums and the release body identify the actual build SHA.
The release-event and explicit selected-tag attachment paths retain their behavior.

## Validation and deployment

The 11 regression cases in `scripts/ci/rolling_release.test.cjs` cover stale-target
selection, first publication, permission/transient failures, immutable releases,
manual uploads, incomplete assets, size/digest mismatch, safe cleanup and races.
They run in both Java regression CI and the publication job. Local checks passed:

```sh
node scripts/ci/rolling_release.test.cjs
node --test scripts/portable/check_webui_checkout.mjs scripts/ci/rolling_release.test.cjs
actionlint -shellcheck= .github/workflows/portable-release.yml .github/workflows/java-regressions.yml
```

Workflow lint used checksum-verified upstream actionlint 1.7.12. The application's
runtime, version, build jobs and package formats were not changed. No release,
asset or tag was changed remotely. Live publication still needs verification after
the fix is applied to `master` by the owner.

An ordinary re-run of the old failed job uses its original workflow and will not
pick up this patch. Apply the fix before triggering a new portable publication.
For urgent recovery without rebuilding, the owner can download the successful
artifacts from the failed run and upload verified packages with `gh release upload`;
that endpoint does not update release metadata. Avoid another run of the original
pre-delete step. The local patch remains on `jvm-sfx-native`; no push or merge was
performed under the standing execution instructions.
