# Portable build caching and publication

Portable Packages builds BLAST and SeqKit/ModDotPlot concurrently on each of the
four supported platforms. Each component has an independent compiled-payload
cache. The verified components are combined into the same artifact layout used
by the package builders. Component assembly checks every file checksum and
rejects missing files, overlapping files/commands, and mismatched platforms.

Cache keys include the component, platform/architecture, hosted runner image,
Python version, Go version, pinned source/configuration files, build scripts,
licences and validation code. Linux still builds in the pinned manylinux2014
container and audits GLIBC compatibility; macOS still audits deployment targets.
A cold build runs the existing fixtures. A warm build checks the entire payload
inventory and hashes, then reruns native SeqKit, ModDotPlot and BLAST known-answer
fixtures. Windows additionally retains its relocation check.

The existing hictk/minimap2/mm2-plus and JNI caches now require exact keys;
partial matches are discarded before building. Keys include build configuration,
runner image and source inputs. The tool cache also fingerprints upstream Git
refs, including peeled annotated tags, and checks that they stayed fixed before
saving. Moving a tag therefore invalidates its cache. Windows uses one complete
tool cache rather than overlapping mm2-plus and whole-toolchain caches.
Dependency caches (Conan, Gradle and Cargo) retain their existing reuse behavior.
Cache write failures remain nonfatal: a later run can rebuild. Corrupt compiled
cache hits fail validation; remove the affected cache entry and rerun.

The default publication order is:

1. Build/restore and validate bundled sequence tools.
2. Build HiCT packages, retaining each platform's existing package checks.
3. Run JVM/native regression tests on Linux, Windows and both macOS architectures.
4. Publish only when every platform build and the regression gate succeeded.

For a manual run, **Publish before JVM regression tests finish** is an unchecked
boolean input. Enabling it allows publication after all package jobs succeed,
while JVM regressions run independently. It never skips the tests or bypasses
failed package builds. Later regression failures still fail the workflow but do
not retract a release already published using this explicit override.
Artifact-only runs and fix-branch release safeguards retain their behavior.

A first run after these changes fills new caches. Subsequent matching runs avoid
recompiling the cached payloads. Hosted timing and all-platform validation must
be measured from the actual Actions runs; local checks do not establish them.
