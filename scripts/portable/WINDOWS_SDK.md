# Windows EXE packaging SDK

The portable EXE requires the official LZMA SDK extractor/SFX modules. Version
26.01 is pinned to SHA-256
`b860f17f9df3c0524dd2ef2c639ab5e43ad0006b77b8f7bb6d191bf528536885`.
The [official GitHub release](https://github.com/ip7z/7zip/releases/tag/26.01)
is tried first, with `https://www.7-zip.org/a/lzma2601.7z` as an alternate host.
Each source gets at most two attempts, with a 60-second request timeout.

Verified archives persist in `.cache/lzma-sdk/<sha256>.7z`, outside the disposable
EXE staging directory, and GitHub Actions caches them. Every cache hit is hashed
again. Downloads enter the cache only after successful verification. Invalid
cache entries are preserved with a `.rejected-<id>` suffix; partial downloads
are never reused. Extraction uses a fresh directory and verifies the required
extractor and SDK notice before packaging. The notice remains in the package.

For an offline build, put the official archive at that checksum-named path, or
set `HICT_LZMA_SDK_CACHE_DIR` to a populated cache directory. A custom mirror can
be selected with `SEVENZIP_SDK_URL`; when changing SDK version, also provide its
expected digest with `SEVENZIP_SDK_SHA256`. A custom digest disables the default
release mirrors, so incompatible versions cannot be silently substituted.

Run the network-independent PowerShell tests with:

```powershell
./scripts/portable/test_download_verified.ps1
```

Pushes to `fix/*` branches run the normal portable build and upload workflow
artifacts, but skip release publication (including rolling-asset deletion).
These preview builds use the maintained `jvm-sfx-native` WebUI branch; manual
workflow runs can still pin a specific WebUI commit with `webui_ref`.
