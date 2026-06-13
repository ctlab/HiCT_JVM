# HiCT_JVM / JHDF5 integration notes

Merging and publishing the JHDF5 repository is necessary, but it is not sufficient by itself.
HiCT_JVM must also resolve and use the packaged `sis-jhdf5-19.04.1.jar` produced by the same JHDF5 branch/build.
Otherwise Gradle can silently keep using `cisd:jhdf5:19.04.1` from Maven, which does not contain the fixed Windows/macOS/Linux native payload layout needed by the portable packages.

## What changed here

- `build.gradle.kts` now honors `HICT_JHDF5_LOCAL_JAR` or Gradle property `hictJhdf5LocalJar`.
- When that jar exists, HiCT_JVM uses `implementation(files(...))` instead of Maven `cisd:jhdf5:19.04.1`.
- Release builds can set `HICT_REQUIRE_BUNDLED_JHDF5=1`; then the build fails instead of falling back to Maven.
- A verification task checks the fat JAR for required JHDF5/HDF5 native entries, including Windows `jhdf5.dll` and macOS `resources/libs/.../libhdf5.dylib` plugin trees.
- `scripts/ci/resolve_jhdf5_jar.sh` and `.ps1` can fetch the packaged jar from the latest successful `build-native.yml` run of the JHDF5 repo branch, or from a GitHub release.

## Recommended CI sequence

After checkout and Java setup, before `./gradlew shadowJar` or any portable package script:

Linux/macOS:

```bash
export HICT_JHDF5_REPO=AxisAlexNT/jhdf5-with-plugins-configuration-snapshot
export HICT_JHDF5_REF="${GITHUB_REF_NAME:-master}"
export HICT_JHDF5_SOURCE_MODE=artifact
export HICT_JHDF5_JAR_NAME=sis-jhdf5-19.04.1.jar
export HICT_JHDF5_LOCAL_JAR=src/main/resources/libs/sis-jhdf5-19.04.1.jar
export HICT_REQUIRE_BUNDLED_JHDF5=1
./scripts/ci/resolve_jhdf5_jar.sh
./gradlew shadowJar verifyBundledJhdf5Payload
```

Windows PowerShell:

```powershell
$env:HICT_JHDF5_REPO = 'AxisAlexNT/jhdf5-with-plugins-configuration-snapshot'
$env:HICT_JHDF5_REF = if ($env:GITHUB_REF_NAME) { $env:GITHUB_REF_NAME } else { 'master' }
$env:HICT_JHDF5_SOURCE_MODE = 'artifact'
$env:HICT_JHDF5_JAR_NAME = 'sis-jhdf5-19.04.1.jar'
$env:HICT_JHDF5_LOCAL_JAR = 'src/main/resources/libs/sis-jhdf5-19.04.1.jar'
$env:HICT_REQUIRE_BUNDLED_JHDF5 = '1'
.\scripts\ci\resolve_jhdf5_jar.ps1
.\gradlew.bat shadowJar verifyBundledJhdf5Payload
```

For release-tag based consumption, set:

```bash
HICT_JHDF5_SOURCE_MODE=release
HICT_JHDF5_RELEASE_TAG=<tag-or-latest>
```

## Important ordering

1. Merge/fix the JHDF5 branch.
2. Run/publish the JHDF5 `build-native.yml` artifacts or GitHub Release assets from that branch.
3. Run HiCT_JVM with `HICT_JHDF5_REF` pointing to that same branch, and `HICT_REQUIRE_BUNDLED_JHDF5=1`.

If step 3 is skipped, HiCT_JVM may still build successfully but with the old Maven JHDF5 jar, so the original native-resource problems can remain.
