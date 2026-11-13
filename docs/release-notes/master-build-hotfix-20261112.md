# Master build failures after the 2.x merge

The merge at `fa4b1cadb456f7471aad7337ad67e85f5d536195` produced two
independent failures. All changes below belong on `jvm-sfx-native` before the
owner merges the hotfix. No master runs were cancelled or restarted.

## Portable Linux build: GNU config transport

[Portable Packages run 43201350428](https://github.com/ctlab/HiCT_JVM/actions/runs/43201350428)
failed while Conan fetched `gnu-config/cci.20210814`, before compiling hictk.
Savannah returned HTTP 500. Windows and both macOS package jobs succeeded;
release publication was skipped because Linux failed.

The unusual `https://https.git.savannah.gnu.org/git/config.git` URL is the
[Conan Center source URL](https://github.com/conan-io/conan-center-index/blob/master/recipes/gnu-config/all/conandata.yml),
not a HiCT string-concatenation error. It responded successfully during the
later investigation, so the evidence indicates an intermittent upstream failure.

POSIX hictk builders now place a small Git wrapper on PATH only for `conan install`.
It maps the GNU config repository's two HTTPS URL forms to the
[GNU config mirror](https://github.com/chipp/gnu-config). Other repositories,
TLS verification, the Conan recipe, dependency versions, patches, and pinned
commit remain unchanged. Git must still fetch and check out the requested object;
there is no fallback to the mirror's latest revision. The mirror is independently
maintained; content identity is constrained by Conan's upstream commit hash.
No user/global Git configuration is written. The wrapper uses `git -c`, avoiding
the newer `GIT_CONFIG_COUNT` interface on older manylinux Git installations.
The wrapper participates in Conan and native-payload cache keys.

Local qualification forced a source build in a fresh Conan home:

- Commit: `191bcb948f7191c36eefe634336f5fc5c0c4c2be`.
- Tree: `aacca35c70f01a5601dd11b29fec0b670a1705bd`.
- Conan's original tvOS patch applied successfully.
- Resulting package revision: `22618e30bd9e326eb95e824dc90cc860`, identical to
  the package downloaded independently from Conan Center in the same test.
- Wrapper URL resolution also passed inside the local manylinux2014 image.

## Autobuild Release: stale JHDF5 artifact lookup

[Autobuild Release run 43201350328](https://github.com/ctlab/HiCT_JVM/actions/runs/43201350328)
looked for an old successful JHDF5 workflow artifact, then switched to Maven.
Dependency locking correctly rejected the unexpected `cisd:jhdf5:19.04.1`
dependency. The lockfile should not be weakened to accommodate this fallback.

Autobuild now resolves the release slim JAR plus split native archive, supplies
its read-only GitHub token to the resolver, and requires successful snapshot
resolution. Missing release payloads fail at resolution instead of silently
changing dependency source. The native branch also triggers Autobuild so this
path is exercised before merging. Its full build and artifact checks remain.

Fresh release downloads passed the resolver's native-entry checks. Gradle
`test testNativeProcessingBaseline shadowJar verifyBundledJhdf5Payload -x copyWebUI`
passed using those assets (unchanged test/build tasks reused their existing
outputs; native-payload verification executed). The toolchain unittest suite
passed 39 tests with one existing skip. New tests cover scoped URL rewriting,
configuration preservation, missing original Git, and failed pinned checkouts.
