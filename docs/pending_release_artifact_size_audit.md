# Pending Release Artifact Size Audit

Network access is restricted in the current run, and the user asked not to request new approvals while away.

To continue the single-file distribution size audit, approve and run:

```bash
mkdir -p /tmp/hict-release-size-audit
curl -L -o /tmp/hict-release-size-audit/HiCT-linux-x86_64.run \
  https://github.com/ctlab/HiCT_JVM/releases/download/latest-master/HiCT-1.0.176-533a1f0-webui_a609177-linux-x86_64.run
curl -L -o /tmp/hict-release-size-audit/HiCT-windows-x86_64.exe \
  https://github.com/ctlab/HiCT_JVM/releases/download/latest-master/HiCT-1.0.176-533a1f0-webui_a609177-windows-x86_64.exe
```

Suggested inspection steps after download:

```bash
ls -lh /tmp/hict-release-size-audit
bash /tmp/hict-release-size-audit/HiCT-linux-x86_64.run --hict-extract-only /tmp/hict-release-size-audit/linux-payload
find /tmp/hict-release-size-audit/linux-payload -type f -printf '%s %p\n' | sort -nr | head -100
du -h -d 4 /tmp/hict-release-size-audit/linux-payload | sort -h | tail -80
```

For Windows `.exe`, inspect with 7-Zip once available:

```bash
7z l /tmp/hict-release-size-audit/HiCT-windows-x86_64.exe
7z x -o/tmp/hict-release-size-audit/windows-payload /tmp/hict-release-size-audit/HiCT-windows-x86_64.exe
find /tmp/hict-release-size-audit/windows-payload -type f -printf '%s %p\n' | sort -nr | head -100
du -h -d 4 /tmp/hict-release-size-audit/windows-payload | sort -h | tail -80
```

Audit goals:

- Check for native payloads from other operating systems or architectures.
- Check duplicated browser/runtime/toolchain directories.
- Check whether bundled JHDF5 natives are stored both as archive and unpacked files.
- Compare compression ratio of the self-extracting wrapper payload against the raw extracted payload.
