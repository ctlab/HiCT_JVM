# Current advisor-release qualification

Historical 2.0.0 release validation is retained separately. It does not qualify
the current advisor-release candidate or current-source platform CI.

The current Linux candidate contains backend version
`2.0.50-a614f88-webui_2041143` and WebUI commit
`2041143e61d8551d610a61c77bb0f55cdc6f9c45`. Its integrated build uses
`./gradlew test shadowJar -PrequireBundledWebUI=true --offline`, without excluding
copyWebUI. The packager requires that version's exact JAR. It must not choose an
arbitrary file from a directory containing earlier builds.

| Candidate artifact | SHA-256 |
|---|---|
| JAR `hict_server-2.0.50-a614f88-webui_2041143-fat.jar` | `e52f06a50a67f01a1336c69c8546ab7db7a559cbfcb20d4daa45e98ad49f5c1d` |
| Linux `.run` | `896279c0d1f7a91a8c7ec6992c78c515325d3176f78510ab32d07552c8eb2f24` |
| Linux `.tar.gz` | `89b54067cdc9ce51ed77c6e0b6d97262cda7655e6cdd2bb45466d745fd8df300` |

The distribution basename is
`HiCT-2.0.50-a614f88-webui_2041143-linux-x86_64`. Artifacts remain local under
build/distributions; they are not published or tagged. Tested extracted package:
`/tmp/hict-integrated-run-extracted-02/HiCT-2.0.50-a614f88-webui_2041143-linux-x86_64`.

## Sequential runtime qualification

`scripts/check_packaged_release_browser.py` launches the extracted package's
bin/hict with its own embedded Java runtime, packaged WebUI and bundled tools,
using fresh /tmp data/processed directories and owned loopback ports. It verifies
the served index and every asset against the package bytes before running browser
stages. It physically copies declared map inputs and generates independent mixed
and structural fixtures. Original inputs are never edited. Fixture preparation
needs Python with Cooler and NumPy, and a host JDK; browser subprocesses need
Playwright and NumPy. `--browser-python` supports a separate browser environment.

Example, from HiCT_JVM, with fresh output and an already verified extraction:

```sh
python scripts/check_packaged_release_browser.py \
  --app /tmp/extracted/HiCT-<version>-linux-x86_64 \
  --output /tmp/hict-packaged-qualification \
  --webui-commit <full-WebUI-commit> \
  --source-map /path/to/physical-copy/medium.hict.hdf5 \
  --source-map /path/to/physical-copy/large.hict.hdf5 \
  --soak-seconds 1800
```

Stages are sequential: version/citation/help metadata, declared menus with real
mouse/keyboard opening at two sizes, structural edits and exact Undo/Redo,
offline editing/session/export/API documentation, explicit navigation performance
gates for the structural and supplied maps, then the full-duration mixed soak.
The soak samples browser/Java heaps, RSS, threads, DOM/listener counts and native
memory while exercising navigation, edits, linked sequence, mixed layers and
exports. The existing numerical and memory limits remain enforced. A shorter
run is diagnostic and is not a thirty-minute qualification. Source and package
JAR hashes must still match after all stages. Server teardown targets only its
owned process group. Running stages update qualification.json and stage logs;
only terminal success supports completion.

Current run: `/tmp/hict-integrated-release-06`. The corrected candidate passes
full JVM checks, checksum verification, self-extraction and the focused picker
regression. Its metadata, 53 served assets and both-size menu checks pass; remaining sequential
stages, including the full soak, are pending. The preceding 49 candidate passed
metadata/menus, exact structural edits, offline recovery and two performance
fixtures; it exposed an incomplete input mislabeled as a real performance map.
The corrected run uses complete 96-, 1,765- and 7,790-contig inputs instead.
Incomplete files now return an actionable input error while retaining the edited
workspace. Running stages are not reported as complete. Final real-example
figures, source-format coverage, platform automation and native GUI checks remain
separate requirements.
