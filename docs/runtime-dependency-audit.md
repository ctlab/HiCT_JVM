# Delivered JVM dependency audit

The audit resolves the JVM runtime classpath, then queries OSV for its exact
Maven package/version pairs. It does not infer HiCT-specific reachability or
cover bundled local JARs, native libraries, sequence tools, browser runtimes or
build-only dependencies. Network failure produces an incomplete report and a
nonzero exit; a completed report with findings also exits nonzero.

```bash
./gradlew hictRuntimeAuditInventory --offline -x copyWebUI -I scripts/runtime_dependency_inventory.init.gradle
python3 scripts/audit_runtime_dependencies.py \
  --inventory build/reports/runtime-dependency-inventory.json \
  --output build/reports/runtime-osv-audit.json
```

Run after dependencies are downloaded. Archive the inventory and dated report
with the exact release source and JAR hash; rerun before final release because
advisories change. Only public package coordinates/versions are submitted.

The November 18, 2026 checkpoint finds eight advisories across five selected
runtime components before updating. Commons Lang is direct; the other four are
transitive. Reviewed Maven Central release metadata and upstream advisories
support these updates:

| Component | Before | Selected | Advisory scope |
|---|---|---|---|
| Commons IO | 2.7 | 2.22.0 | XML reader resource consumption |
| Commons Compress | 1.25.0 | 1.28.0 | DUMP loop and Pack200 allocation |
| Commons Lang | 3.12.0 | 3.22.0 | Uncontrolled recursion |
| JSON-java | 20231013 | 20260814 | Unbounded numeric token processing |
| Snappy-java | 1.1.10.5 | 1.1.10.12 | Native bounds and allocation checks |

Sources: [Commons IO](https://commons.apache.org/proper/commons-io/security.html),
[Commons Compress](https://commons.apache.org/proper/commons-compress/security.html),
[Commons Lang fix](https://lists.apache.org/thread/bgv0lpswokgol11tloxnjfzdl7yrc1g1),
[JSON-java advisory](https://github.com/stleary/JSON-java/security/advisories/GHSA-ghc9-756g-vrcv),
[Snappy native bounds](https://github.com/xerial/snappy-java/security/advisories/GHSA-gwq9-9ffj-mvrv),
[Snappy allocation](https://github.com/xerial/snappy-java/security/advisories/GHSA-q233-g92q-qxp6),
[Snappy copy ranges](https://github.com/xerial/snappy-java/security/advisories/GHSA-xqrx-qh46-34m7).

The updated inventory contains 77 Maven artifacts and 79 files. Its OSV query
completes with zero flagged Maven components. `bigwig-2.0.1.jar` and
`sis-jhdf5-19.04.1-slim.jar` are explicitly listed as uncovered local artifacts.
This is not a blanket security clearance. Before/after inventories, raw advisory
IDs, upstream details and metadata are retained under
`build/release-evidence/2.0.71/` and
`build/release-evidence/runtime-dependency-update/`.

All 661 JVM tests pass (two optional skips), including native Snappy array and
stream round trips and byte-preserving BZip2 compatibility. Native tile parity
also passes. Replacement four-platform CI, scientific browser workflows and
final packaged acceptance are required for the newly selected dependencies.
