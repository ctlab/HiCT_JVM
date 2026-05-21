/*
 * HiCT portable Windows single-file launcher.
 *
 * This program is intentionally small and uses only Win32 APIs plus the C++
 * standard library. It is built with the static MSVC runtime (/MT) by the
 * release packaging script so users do not need a Visual C++ Redistributable.
 *
 * File layout:
 *   launcher.exe || 7zr.exe || payload.7z || manifest.json || u64(manifestSize) || magic
 *
 * The launcher extracts the payload into a per-user, content-addressed cache
 * directory and skips extraction on subsequent launches when the marker matches.
 */

#ifndef UNICODE
#define UNICODE
#endif
#ifndef _UNICODE
#define _UNICODE
#endif

#include <windows.h>
#include <shellapi.h>
#include <shlobj.h>

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cwchar>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

constexpr const char *kFooterMagic = "HICT-PORTABLE-LAUNCHER-V1";
constexpr std::size_t kBufferSize = 1024 * 1024;

struct Manifest {
  std::string appDirName;
  std::string payloadSha256;
  std::string extractorSha256;
  std::uint64_t extractorOffset = 0;
  std::uint64_t extractorSize = 0;
  std::uint64_t payloadOffset = 0;
  std::uint64_t payloadSize = 0;
};

std::wstring utf8ToWide(const std::string &value) {
  if (value.empty()) {
    return L"";
  }
  const int required = MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, value.data(), static_cast<int>(value.size()), nullptr, 0);
  if (required <= 0) {
    throw std::runtime_error("Failed to decode UTF-8 manifest string.");
  }
  std::wstring result(static_cast<std::size_t>(required), L'\0');
  MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, value.data(), static_cast<int>(value.size()), result.data(), required);
  return result;
}

std::string wideToUtf8(const std::wstring &value) {
  if (value.empty()) {
    return "";
  }
  const int required = WideCharToMultiByte(CP_UTF8, 0, value.data(), static_cast<int>(value.size()), nullptr, 0, nullptr, nullptr);
  if (required <= 0) {
    throw std::runtime_error("Failed to encode path as UTF-8.");
  }
  std::string result(static_cast<std::size_t>(required), '\0');
  WideCharToMultiByte(CP_UTF8, 0, value.data(), static_cast<int>(value.size()), result.data(), required, nullptr, nullptr);
  return result;
}

std::wstring getSelfPath() {
  std::wstring buffer(MAX_PATH, L'\0');
  for (;;) {
    const DWORD copied = GetModuleFileNameW(nullptr, buffer.data(), static_cast<DWORD>(buffer.size()));
    if (copied == 0) {
      throw std::runtime_error("GetModuleFileNameW failed.");
    }
    if (copied < buffer.size() - 1) {
      buffer.resize(copied);
      return buffer;
    }
    buffer.resize(buffer.size() * 2);
  }
}

std::wstring dirnameOf(const std::wstring &path) {
  const auto pos = path.find_last_of(L"\\/");
  if (pos == std::wstring::npos) {
    return L".";
  }
  if (pos == 0) {
    return path.substr(0, 1);
  }
  return path.substr(0, pos);
}

std::wstring joinPath(const std::wstring &left, const std::wstring &right) {
  if (left.empty()) {
    return right;
  }
  if (right.empty()) {
    return left;
  }
  if (left.back() == L'\\' || left.back() == L'/') {
    return left + right;
  }
  return left + L"\\" + right;
}

std::wstring quoteArg(const std::wstring &arg) {
  std::wstring result = L"\"";
  std::size_t backslashes = 0;
  for (const wchar_t ch : arg) {
    if (ch == L'\\') {
      backslashes++;
      continue;
    }
    if (ch == L'"') {
      result.append(backslashes * 2 + 1, L'\\');
      result.push_back(ch);
      backslashes = 0;
      continue;
    }
    result.append(backslashes, L'\\');
    backslashes = 0;
    result.push_back(ch);
  }
  result.append(backslashes * 2, L'\\');
  result.push_back(L'"');
  return result;
}

bool fileExists(const std::wstring &path) {
  const DWORD attr = GetFileAttributesW(path.c_str());
  return attr != INVALID_FILE_ATTRIBUTES && (attr & FILE_ATTRIBUTE_DIRECTORY) == 0;
}

bool directoryExists(const std::wstring &path) {
  const DWORD attr = GetFileAttributesW(path.c_str());
  return attr != INVALID_FILE_ATTRIBUTES && (attr & FILE_ATTRIBUTE_DIRECTORY) != 0;
}

void createDirectories(const std::wstring &path) {
  if (path.empty() || directoryExists(path)) {
    return;
  }
  std::wstring normalized = path;
  for (wchar_t &ch : normalized) {
    if (ch == L'/') {
      ch = L'\\';
    }
  }

  std::size_t start = 0;
  if (normalized.size() >= 2 && normalized[1] == L':') {
    start = 3;
  } else if (normalized.rfind(L"\\\\", 0) == 0) {
    const auto first = normalized.find(L'\\', 2);
    const auto second = first == std::wstring::npos ? std::wstring::npos : normalized.find(L'\\', first + 1);
    start = second == std::wstring::npos ? normalized.size() : second + 1;
  }

  for (std::size_t pos = start; pos <= normalized.size(); ++pos) {
    if (pos != normalized.size() && normalized[pos] != L'\\') {
      continue;
    }
    const std::wstring part = normalized.substr(0, pos);
    if (part.empty() || directoryExists(part)) {
      continue;
    }
    if (!CreateDirectoryW(part.c_str(), nullptr) && GetLastError() != ERROR_ALREADY_EXISTS) {
      std::wcerr << L"Failed to create directory: " << part << L"\n";
      throw std::runtime_error("CreateDirectoryW failed.");
    }
  }
}

void removeTreeIfExists(const std::wstring &path) {
  if (!directoryExists(path)) {
    return;
  }
  std::wstring doubleNull = path;
  doubleNull.push_back(L'\0');
  doubleNull.push_back(L'\0');
  SHFILEOPSTRUCTW operation{};
  operation.wFunc = FO_DELETE;
  operation.pFrom = doubleNull.c_str();
  operation.fFlags = FOF_NOCONFIRMATION | FOF_NOERRORUI | FOF_SILENT;
  SHFileOperationW(&operation);
}

std::wstring getWritableCacheRoot() {
  wchar_t localAppData[MAX_PATH]{};
  if (SUCCEEDED(SHGetFolderPathW(nullptr, CSIDL_LOCAL_APPDATA | CSIDL_FLAG_CREATE, nullptr, SHGFP_TYPE_CURRENT, localAppData))) {
    return joinPath(localAppData, L"HiCT\\portable");
  }

  DWORD needed = GetTempPathW(0, nullptr);
  if (needed == 0) {
    return L".";
  }
  std::wstring temp(needed, L'\0');
  GetTempPathW(needed, temp.data());
  temp.resize(std::wcslen(temp.c_str()));
  return joinPath(temp, L"HiCT\\portable");
}

std::uint64_t fileSize(FILE *file) {
  if (_fseeki64(file, 0, SEEK_END) != 0) {
    throw std::runtime_error("Failed to seek to executable end.");
  }
  const auto size = _ftelli64(file);
  if (size < 0) {
    throw std::runtime_error("Failed to determine executable size.");
  }
  return static_cast<std::uint64_t>(size);
}

std::string readSegment(FILE *file, std::uint64_t offset, std::uint64_t size) {
  if (size > static_cast<std::uint64_t>(64 * 1024 * 1024)) {
    throw std::runtime_error("Refusing to read unexpectedly large metadata segment.");
  }
  if (_fseeki64(file, static_cast<__int64>(offset), SEEK_SET) != 0) {
    throw std::runtime_error("Failed to seek to metadata segment.");
  }
  std::string data(static_cast<std::size_t>(size), '\0');
  if (size > 0 && std::fread(data.data(), 1, static_cast<std::size_t>(size), file) != size) {
    throw std::runtime_error("Failed to read metadata segment.");
  }
  return data;
}

std::uint64_t readLe64(const unsigned char *bytes) {
  std::uint64_t value = 0;
  for (int i = 7; i >= 0; --i) {
    value = (value << 8) | bytes[i];
  }
  return value;
}

std::string parseJsonString(const std::string &json, const std::string &key) {
  const std::string needle = "\"" + key + "\"";
  const auto keyPos = json.find(needle);
  if (keyPos == std::string::npos) {
    throw std::runtime_error("Missing manifest key: " + key);
  }
  const auto colon = json.find(':', keyPos + needle.size());
  const auto firstQuote = json.find('"', colon + 1);
  if (colon == std::string::npos || firstQuote == std::string::npos) {
    throw std::runtime_error("Malformed manifest string key: " + key);
  }
  std::string result;
  bool escaped = false;
  for (std::size_t i = firstQuote + 1; i < json.size(); ++i) {
    const char ch = json[i];
    if (escaped) {
      result.push_back(ch);
      escaped = false;
      continue;
    }
    if (ch == '\\') {
      escaped = true;
      continue;
    }
    if (ch == '"') {
      return result;
    }
    result.push_back(ch);
  }
  throw std::runtime_error("Unterminated manifest string key: " + key);
}

std::uint64_t parseJsonUInt64(const std::string &json, const std::string &key) {
  const std::string needle = "\"" + key + "\"";
  const auto keyPos = json.find(needle);
  if (keyPos == std::string::npos) {
    throw std::runtime_error("Missing manifest key: " + key);
  }
  const auto colon = json.find(':', keyPos + needle.size());
  if (colon == std::string::npos) {
    throw std::runtime_error("Malformed manifest numeric key: " + key);
  }
  auto pos = colon + 1;
  while (pos < json.size() && (json[pos] == ' ' || json[pos] == '\n' || json[pos] == '\r' || json[pos] == '\t')) {
    pos++;
  }
  std::uint64_t value = 0;
  bool foundDigit = false;
  while (pos < json.size() && json[pos] >= '0' && json[pos] <= '9') {
    foundDigit = true;
    value = value * 10 + static_cast<std::uint64_t>(json[pos] - '0');
    pos++;
  }
  if (!foundDigit) {
    throw std::runtime_error("Malformed manifest numeric key: " + key);
  }
  return value;
}

Manifest readManifest(const std::wstring &selfPath) {
  FILE *file = nullptr;
  if (_wfopen_s(&file, selfPath.c_str(), L"rb") != 0 || file == nullptr) {
    throw std::runtime_error("Failed to open launcher executable.");
  }

  try {
    const auto totalSize = fileSize(file);
    const std::size_t magicSize = std::strlen(kFooterMagic);
    if (totalSize < magicSize + 8) {
      throw std::runtime_error("Launcher metadata footer is missing.");
    }

    if (_fseeki64(file, static_cast<__int64>(totalSize - magicSize - 8), SEEK_SET) != 0) {
      throw std::runtime_error("Failed to seek to launcher footer.");
    }
    unsigned char lengthBytes[8]{};
    if (std::fread(lengthBytes, 1, sizeof(lengthBytes), file) != sizeof(lengthBytes)) {
      throw std::runtime_error("Failed to read launcher manifest length.");
    }
    std::string magic(magicSize, '\0');
    if (std::fread(magic.data(), 1, magicSize, file) != magicSize || magic != kFooterMagic) {
      throw std::runtime_error("Launcher metadata magic does not match.");
    }

    const auto manifestSize = readLe64(lengthBytes);
    if (manifestSize > totalSize - magicSize - 8) {
      throw std::runtime_error("Launcher manifest size is invalid.");
    }
    const auto manifestOffset = totalSize - magicSize - 8 - manifestSize;
    const auto manifestText = readSegment(file, manifestOffset, manifestSize);

    Manifest manifest;
    manifest.appDirName = parseJsonString(manifestText, "appDirName");
    manifest.payloadSha256 = parseJsonString(manifestText, "payloadSha256");
    manifest.extractorSha256 = parseJsonString(manifestText, "extractorSha256");
    manifest.extractorOffset = parseJsonUInt64(manifestText, "extractorOffset");
    manifest.extractorSize = parseJsonUInt64(manifestText, "extractorSize");
    manifest.payloadOffset = parseJsonUInt64(manifestText, "payloadOffset");
    manifest.payloadSize = parseJsonUInt64(manifestText, "payloadSize");

    if (manifest.extractorOffset > manifestOffset ||
        manifest.extractorSize > manifestOffset - manifest.extractorOffset ||
        manifest.payloadOffset > manifestOffset ||
        manifest.payloadSize > manifestOffset - manifest.payloadOffset) {
      throw std::runtime_error("Launcher embedded payload offsets are invalid.");
    }
    std::fclose(file);
    return manifest;
  } catch (...) {
    std::fclose(file);
    throw;
  }
}

void copySegmentToFile(const std::wstring &selfPath,
                       std::uint64_t offset,
                       std::uint64_t size,
                       const std::wstring &targetPath) {
  createDirectories(dirnameOf(targetPath));

  FILE *source = nullptr;
  if (_wfopen_s(&source, selfPath.c_str(), L"rb") != 0 || source == nullptr) {
    throw std::runtime_error("Failed to open launcher for embedded payload extraction.");
  }
  FILE *target = nullptr;
  if (_wfopen_s(&target, targetPath.c_str(), L"wb") != 0 || target == nullptr) {
    std::fclose(source);
    throw std::runtime_error("Failed to create embedded payload target file.");
  }

  std::vector<char> buffer(kBufferSize);
  try {
    if (_fseeki64(source, static_cast<__int64>(offset), SEEK_SET) != 0) {
      throw std::runtime_error("Failed to seek to embedded payload.");
    }
    std::uint64_t remaining = size;
    while (remaining > 0) {
      const auto chunk = static_cast<std::size_t>(remaining < buffer.size() ? remaining : buffer.size());
      const auto read = std::fread(buffer.data(), 1, chunk, source);
      if (read != chunk) {
        throw std::runtime_error("Failed to read embedded payload chunk.");
      }
      if (std::fwrite(buffer.data(), 1, chunk, target) != chunk) {
        throw std::runtime_error("Failed to write embedded payload chunk.");
      }
      remaining -= chunk;
    }
    std::fclose(source);
    std::fclose(target);
  } catch (...) {
    std::fclose(source);
    std::fclose(target);
    throw;
  }
}

std::string readSmallTextFile(const std::wstring &path) {
  FILE *file = nullptr;
  if (_wfopen_s(&file, path.c_str(), L"rb") != 0 || file == nullptr) {
    return "";
  }
  char buffer[256]{};
  const auto bytes = std::fread(buffer, 1, sizeof(buffer) - 1, file);
  std::fclose(file);
  return std::string(buffer, bytes);
}

void writeSmallTextFile(const std::wstring &path, const std::string &text) {
  FILE *file = nullptr;
  if (_wfopen_s(&file, path.c_str(), L"wb") != 0 || file == nullptr) {
    throw std::runtime_error("Failed to write cache marker.");
  }
  std::fwrite(text.data(), 1, text.size(), file);
  std::fclose(file);
}

int runProcess(const std::wstring &commandLine, const std::wstring &workingDirectory) {
  std::vector<wchar_t> mutableCommand(commandLine.begin(), commandLine.end());
  mutableCommand.push_back(L'\0');

  STARTUPINFOW startup{};
  startup.cb = sizeof(startup);
  PROCESS_INFORMATION process{};
  const BOOL ok = CreateProcessW(
      nullptr,
      mutableCommand.data(),
      nullptr,
      nullptr,
      TRUE,
      0,
      nullptr,
      workingDirectory.empty() ? nullptr : workingDirectory.c_str(),
      &startup,
      &process);
  if (!ok) {
    std::wcerr << L"Failed to start process: " << commandLine << L"\n";
    return 1;
  }
  WaitForSingleObject(process.hProcess, INFINITE);
  DWORD exitCode = 1;
  GetExitCodeProcess(process.hProcess, &exitCode);
  CloseHandle(process.hThread);
  CloseHandle(process.hProcess);
  return static_cast<int>(exitCode);
}

std::wstring getDataDirectory(const std::wstring &selfPath) {
  DWORD existingSize = GetEnvironmentVariableW(L"DATA_DIR", nullptr, 0);
  if (existingSize > 0) {
    std::wstring existing(existingSize, L'\0');
    GetEnvironmentVariableW(L"DATA_DIR", existing.data(), existingSize);
    existing.resize(std::wcslen(existing.c_str()));
    return existing;
  }
  const auto selfDir = dirnameOf(selfPath);
  SetEnvironmentVariableW(L"DATA_DIR", selfDir.c_str());
  return selfDir;
}

std::wstring buildForwardedArguments() {
  int argc = 0;
  LPWSTR *argv = CommandLineToArgvW(GetCommandLineW(), &argc);
  if (argv == nullptr) {
    return L"";
  }
  std::wstring result;
  for (int i = 1; i < argc; ++i) {
    if (!result.empty()) {
      result.push_back(L' ');
    }
    result += quoteArg(argv[i]);
  }
  LocalFree(argv);
  return result;
}

bool isConsoleOwnedByThisProcess() {
  DWORD processes[4]{};
  const DWORD count = GetConsoleProcessList(processes, 4);
  return count <= 1;
}

void pauseOnFailureIfNeeded(int exitCode) {
  if (exitCode == 0 || !isConsoleOwnedByThisProcess()) {
    return;
  }
  std::wcerr << L"\nHiCT exited with code " << exitCode << L". Press Enter to close this window...";
  std::wstring line;
  std::getline(std::wcin, line);
}

void printUsage() {
  std::wcout
      << L"HiCT portable Windows launcher\n\n"
      << L"Usage:\n"
      << L"  HiCT-<version>-windows-x86_64.exe [HiCT CLI args...]\n"
      << L"  HiCT-<version>-windows-x86_64.exe --hict-extract-only <directory>\n\n"
      << L"DATA_DIR defaults to the directory containing this EXE unless explicitly set.\n";
}

int extractPayload(const std::wstring &selfPath,
                   const Manifest &manifest,
                   const std::wstring &targetRoot,
                   bool launchAfterExtract) {
  createDirectories(targetRoot);
  const auto extractorPath = joinPath(targetRoot, L"7zr.exe");
  const auto archivePath = joinPath(targetRoot, L"payload.7z");
  const auto markerPath = joinPath(targetRoot, L".payload.sha256");
  const auto appHome = joinPath(targetRoot, utf8ToWide(manifest.appDirName));

  if (launchAfterExtract) {
    const auto cachedMarker = readSmallTextFile(markerPath);
    if (cachedMarker == manifest.payloadSha256 && fileExists(joinPath(appHome, L"HiCT.cmd"))) {
      return 0;
    }
    removeTreeIfExists(targetRoot);
    createDirectories(targetRoot);
  }

  std::wcout << L"Preparing HiCT portable files in " << targetRoot << L"\n";
  copySegmentToFile(selfPath, manifest.extractorOffset, manifest.extractorSize, extractorPath);
  copySegmentToFile(selfPath, manifest.payloadOffset, manifest.payloadSize, archivePath);

  const std::wstring extractCommand =
      quoteArg(extractorPath) + L" x " + quoteArg(archivePath) + L" -o" + quoteArg(targetRoot) + L" -y";
  const int extractExit = runProcess(extractCommand, targetRoot);
  if (extractExit != 0) {
    std::wcerr << L"Failed to extract HiCT payload. 7-Zip exit code: " << extractExit << L"\n";
    return extractExit;
  }
  DeleteFileW(archivePath.c_str());
  writeSmallTextFile(markerPath, manifest.payloadSha256);
  return 0;
}

int run() {
  const auto selfPath = getSelfPath();
  const auto manifest = readManifest(selfPath);

  int argc = 0;
  LPWSTR *argv = CommandLineToArgvW(GetCommandLineW(), &argc);
  if (argv != nullptr && argc >= 2 && std::wcscmp(argv[1], L"--hict-run-help") == 0) {
    printUsage();
    LocalFree(argv);
    return 0;
  }
  if (argv != nullptr && argc >= 3 && std::wcscmp(argv[1], L"--hict-extract-only") == 0) {
    const std::wstring target = argv[2];
    LocalFree(argv);
    return extractPayload(selfPath, manifest, target, false);
  }
  if (argv != nullptr) {
    LocalFree(argv);
  }

  const auto cacheRoot = joinPath(getWritableCacheRoot(), utf8ToWide(manifest.appDirName + "-" + manifest.payloadSha256));
  const int extractExit = extractPayload(selfPath, manifest, cacheRoot, true);
  if (extractExit != 0) {
    pauseOnFailureIfNeeded(extractExit);
    return extractExit;
  }

  const auto appHome = joinPath(cacheRoot, utf8ToWide(manifest.appDirName));
  const auto hictCmd = joinPath(appHome, L"HiCT.cmd");
  if (!fileExists(hictCmd)) {
    std::wcerr << L"HiCT portable payload is incomplete: " << hictCmd << L" was not found.\n";
    pauseOnFailureIfNeeded(1);
    return 1;
  }

  const auto dataDir = getDataDirectory(selfPath);
  createDirectories(dataDir);
  SetEnvironmentVariableW(L"HICT_PORTABLE_EXE_PATH", selfPath.c_str());
  SetEnvironmentVariableW(L"HICT_PORTABLE_CACHE_DIR", cacheRoot.c_str());

  const auto args = buildForwardedArguments();
  std::wstring command = L"cmd.exe /d /c call " + quoteArg(hictCmd);
  if (!args.empty()) {
    command.push_back(L' ');
    command += args;
  }
  const int exitCode = runProcess(command, dataDir);
  pauseOnFailureIfNeeded(exitCode);
  return exitCode;
}

} // namespace

int wmain() {
  try {
    return run();
  } catch (const std::exception &e) {
    std::cerr << "HiCT portable launcher failed: " << e.what() << "\n";
    pauseOnFailureIfNeeded(1);
    return 1;
  }
}
