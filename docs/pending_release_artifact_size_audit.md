# Release Artifact Size Audit

Status: completed locally after approval on 2026-06-15.

Downloaded artifacts:

- Linux runfile: `HiCT-1.0.176-533a1f0-webui_a609177-linux-x86_64.run`
- Windows self-extractor: `HiCT-1.0.176-533a1f0-webui_a609177-windows-x86_64.exe`

Commands used:

```bash
mkdir -p /tmp/hict-release-size-audit
curl -L -o /tmp/hict-release-size-audit/HiCT-linux-x86_64.run \
  https://github.com/ctlab/HiCT_JVM/releases/download/latest-master/HiCT-1.0.176-533a1f0-webui_a609177-linux-x86_64.run
curl -L -o /tmp/hict-release-size-audit/HiCT-windows-x86_64.exe \
  https://github.com/ctlab/HiCT_JVM/releases/download/latest-master/HiCT-1.0.176-533a1f0-webui_a609177-windows-x86_64.exe
```

Inspection commands:

```bash
ls -lh /tmp/hict-release-size-audit
bash /tmp/hict-release-size-audit/HiCT-linux-x86_64.run --hict-extract-only /tmp/hict-release-size-audit/linux-payload
find /tmp/hict-release-size-audit/linux-payload -type f -printf '%s %p\n' | sort -nr | head -100
du -h -d 4 /tmp/hict-release-size-audit/linux-payload | sort -h | tail -80
7z l /tmp/hict-release-size-audit/HiCT-windows-x86_64.exe
7z x -o/tmp/hict-release-size-audit/windows-payload /tmp/hict-release-size-audit/HiCT-windows-x86_64.exe
find /tmp/hict-release-size-audit/windows-payload -type f -printf '%s %p\n' | sort -nr | head -100
du -h -d 4 /tmp/hict-release-size-audit/windows-payload | sort -h | tail -80
```

Findings:

- Published Linux `.run`: 299 MiB compressed, 550 MiB extracted.
- Published Windows `.exe`: 304 MiB compressed, 582 MiB extracted.
- Main extracted size contributor on both platforms is Electron:
  - Linux: `browsers/linux_x86_64/electron` is about 257 MiB.
  - Windows: `browsers/windows_x86_64/electron` is about 303 MiB.
- The fat JAR duplicated JHDF5 native archives:
  - one full `libs/sis-jhdf5-19.04.1-natives.tar.gz` entry around 50 MiB,
  - one pruned `libs/sis-jhdf5-19.04.1-natives.tar.gz` entry around 41 MiB,
  - plus a copied sidecar archive in portable `lib/`.
- No evidence of full cross-OS browser/toolchain directories in the inspected platform payloads.

Implemented fix:

- `processResources` now embeds only the pruned runtime JHDF5 native archive.
- `shadowJar` no longer adds a second archive entry and deletes stale sidecar archives from `build/libs`.
- Linux, Windows, and macOS portable packagers no longer copy the JHDF5 native archive beside `hict.jar`; the runtime loader uses the embedded archive.

Measured result after fix on local Linux build:

- Fat JAR: about 87 MiB, down from about 141 MiB.
- Linux extracted portable payload: about 451 MiB, down from about 550 MiB.
- Linux `.run`: about 209 MiB, down from 299 MiB.
- The smoke test loaded HDF5 from the embedded archive successfully.

Remaining optional size tradeoff:

- Electron is still the largest component. Removing it would save roughly 250-300 MiB extracted and a large portion of compressed size, but would remove the fallback browser path for systems where Tauri/WebView or a system browser is unavailable.
