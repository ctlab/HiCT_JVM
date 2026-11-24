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
 * The launcher prefers a per-user cache and can also use a cache next to the
 * EXE or under the OS temp directory.
 * Subsequent launches skip extraction when the marker matches.
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
#include <cctype>
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

std::wstring getWritableCacheRoot(const std::wstring &selfPath) {
  return joinPath(dirnameOf(selfPath), L"HiCT.portable\\payloads");
}

std::wstring getTempDirectory() {
  const DWORD required = GetTempPathW(0, nullptr);
  if (required == 0) {
    return L".";
  }
  std::wstring buffer(required + 1, L'\0');
  const DWORD copied = GetTempPathW(static_cast<DWORD>(buffer.size()), buffer.data());
  if (copied == 0) {
    return L".";
  }
  buffer.resize(std::wcslen(buffer.c_str()));
  return buffer;
}

std::wstring getFallbackCacheRoot() {
  return joinPath(getTempDirectory(), L"HiCT.portable\\payloads");
}

std::wstring getLocalHiCTDirectory() {
  wchar_t localAppData[MAX_PATH]{};
  if (FAILED(SHGetFolderPathW(nullptr, CSIDL_LOCAL_APPDATA | CSIDL_FLAG_DONT_VERIFY,
                            nullptr, SHGFP_TYPE_CURRENT, localAppData))) {
    return L"";
  }
  return joinPath(localAppData, L"HiCT");
}

std::wstring chooseWorkingFolder(const wchar_t *message) {
  BROWSEINFOW info{};
  info.lpszTitle = message;
  info.ulFlags = BIF_RETURNONLYFSDIRS;
  const auto item = SHBrowseForFolderW(&info);
  if (item == nullptr) return L"";
  wchar_t path[MAX_PATH]{};
  const bool found = SHGetPathFromIDListW(item, path);
  CoTaskMemFree(item);
  return found ? path : L"";
}

bool environmentVariableIsDefined(const wchar_t *name) {
  return GetEnvironmentVariableW(name, nullptr, 0) > 0;
}

void defaultEnvironment(const wchar_t *name, const std::wstring &value) {
  if (!environmentVariableIsDefined(name)) {
    SetEnvironmentVariableW(name, value.c_str());
  }
}

std::string shortPayloadCacheKey(const Manifest &manifest) {
  std::string key;
  key.reserve(18);
  key += "p2-";
  for (const char ch : manifest.payloadSha256) {
    const bool hex = (ch >= '0' && ch <= '9') ||
        (ch >= 'a' && ch <= 'f') ||
        (ch >= 'A' && ch <= 'F');
    if (hex) {
      key.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(ch))));
    }
    if (key.size() >= 18) {
      break;
    }
  }
  if (key.size() <= 3) {
    key += "unknown";
  }
  return key;
}

void printStartupHeader() {
  std::wcout
      << L"HiCT portable launcher\n"
      << L"Authors: Aleksandr Serdiukov, Anton Zamyatin, Aleksandr Sinitsyn, Vitalii Dravgelis,\n"
      << L"         Zakhar Lobanov, Nikita Zheleznov, Pavel Avdeyev, Nikolay Cherkasov,\n"
      << L"         and Computer Technologies Laboratory ITMO University team.\n"
      << L"License: MIT. Third-party notices are included in licenses/, runtime/legal, and toolchains/.\n\n";
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
  const auto written = std::fwrite(text.data(), 1, text.size(), file);
  const auto closed = std::fclose(file);
  if (written != text.size() || closed != 0) {
    throw std::runtime_error("Failed to finish writing the cache marker.");
  }
}

int runProcess(const std::wstring &commandLine, const std::wstring &workingDirectory, bool quiet = false) {
  std::vector<wchar_t> mutableCommand(commandLine.begin(), commandLine.end());
  mutableCommand.push_back(L'\0');

  STARTUPINFOW startup{};
  startup.cb = sizeof(startup);
  HANDLE sink = INVALID_HANDLE_VALUE;
  if (quiet) {
    SECURITY_ATTRIBUTES inherited{sizeof(SECURITY_ATTRIBUTES), nullptr, TRUE};
    sink = CreateFileW(L"NUL", GENERIC_READ | GENERIC_WRITE, FILE_SHARE_READ | FILE_SHARE_WRITE,
                       &inherited, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
    if (sink != INVALID_HANDLE_VALUE) {
      startup.dwFlags = STARTF_USESTDHANDLES;
      startup.hStdInput = startup.hStdOutput = startup.hStdError = sink;
    }
  }
  PROCESS_INFORMATION process{};
  const BOOL ok = CreateProcessW(
      nullptr,
      mutableCommand.data(),
      nullptr,
      nullptr,
      TRUE,
      quiet ? CREATE_NO_WINDOW : 0,
      nullptr,
      workingDirectory.empty() ? nullptr : workingDirectory.c_str(),
      &startup,
      &process);
  const auto error = ok ? ERROR_SUCCESS : GetLastError();
  if (sink != INVALID_HANDLE_VALUE) CloseHandle(sink);
  if (!ok) {
    std::wcerr << L"Failed to start process (Windows error " << error << L"): " << commandLine << L"\n";
    return 1;
  }
  WaitForSingleObject(process.hProcess, INFINITE);
  DWORD exitCode = 1;
  GetExitCodeProcess(process.hProcess, &exitCode);
  CloseHandle(process.hThread);
  CloseHandle(process.hProcess);
  return static_cast<int>(exitCode);
}

bool canLaunchBundledJava(const std::wstring &cache, const Manifest &manifest) {
  const auto appHome = joinPath(cache, utf8ToWide(manifest.appDirName));
  const auto java = joinPath(appHome, L"runtime\\bin\\java.exe");
  const auto status = runProcess(quoteArg(java) + L" -version", appHome, true);
  if (status == 0) return true;
  std::wcerr << L"The bundled Java executable could not run from " << appHome
             << L" (exit " << status << L"). Trying another cache.\n";
  return false;
}

int extractPayload(const std::wstring &selfPath,
                   const Manifest &manifest,
                   const std::wstring &targetRoot,
                   bool launchAfterExtract);

int tryExtractPayload(const std::wstring &selfPath,
                      const Manifest &manifest,
                      const std::wstring &targetRoot,
                      bool launchAfterExtract) {
  try {
    return extractPayload(selfPath, manifest, targetRoot, launchAfterExtract);
  } catch (const std::exception &e) {
    std::cerr << "Failed to prepare HiCT payload cache: " << e.what() << "\n";
    return 1;
  }
}

class CacheLock {
  HANDLE handle_;

public:
  explicit CacheLock(const std::wstring &path) : handle_(INVALID_HANDLE_VALUE) {
    // The extractor inherits this handle. A killed launcher cannot release the
    // cache to another process while its extractor is still writing.
    SECURITY_ATTRIBUTES inherited{sizeof(SECURITY_ATTRIBUTES), nullptr, TRUE};
    const auto deadline = GetTickCount64() + 300'000;
    do {
      handle_ = CreateFileW(path.c_str(), GENERIC_WRITE, 0, &inherited, OPEN_ALWAYS,
                            FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OPEN_REPARSE_POINT, nullptr);
      if (handle_ != INVALID_HANDLE_VALUE) return;
      const auto error = GetLastError();
      if (error != ERROR_SHARING_VIOLATION) {
        throw std::runtime_error("Cannot lock payload cache (Windows error " + std::to_string(error) + ").");
      }
      Sleep(100);
    } while (GetTickCount64() < deadline);
    throw std::runtime_error("Timed out waiting for another HiCT launch to finish extracting.");
  }

  ~CacheLock() { CloseHandle(handle_); }
  CacheLock(const CacheLock &) = delete;
  CacheLock &operator=(const CacheLock &) = delete;
};

std::wstring getEnvironmentVariableValue(const wchar_t *name) {
  const DWORD size = GetEnvironmentVariableW(name, nullptr, 0);
  if (size == 0) {
    return L"";
  }
  std::wstring value(size, L'\0');
  GetEnvironmentVariableW(name, value.data(), size);
  value.resize(std::wcslen(value.c_str()));
  return value;
}

bool canWritePrivateFile(const std::wstring &directory) {
  if (directory.empty()) {
    return false;
  }
  try {
    createDirectories(directory);
    const auto probePath = joinPath(directory, L".hict-write-test-" + std::to_wstring(GetCurrentProcessId()));
    const HANDLE probe = CreateFileW(probePath.c_str(), GENERIC_WRITE | DELETE, 0, nullptr,
                                    CREATE_NEW, FILE_ATTRIBUTE_TEMPORARY | FILE_FLAG_DELETE_ON_CLOSE, nullptr);
    if (probe == INVALID_HANDLE_VALUE) {
      const auto error = GetLastError();
      std::wcerr << L"Cannot create a runtime file in " << directory << L" (Windows error " << error << L").\n";
      return false;
    }
    DWORD written = 0;
    const bool usable = WriteFile(probe, "1", 1, &written, nullptr) && written == 1;
    CloseHandle(probe);
    return usable;
  } catch (...) {
    return false;
  }
}

std::wstring selectRuntimeTempDir(const std::wstring &dataDir, const std::wstring &appHome) {
  const auto localHiCTDir = getLocalHiCTDirectory();
  const auto configuredTempDir = getEnvironmentVariableValue(L"HICT_TEMP_DIR");
  const std::vector<std::wstring> candidates{
      configuredTempDir,
      localHiCTDir.empty() ? L"" : joinPath(localHiCTDir, L"runtime"),
      joinPath(getTempDirectory(), L"HiCT\\runtime"),
      joinPath(dataDir, L"tmp"),
      joinPath(appHome, L"tmp")};

  for (const auto &candidate : candidates) {
    if (!candidate.empty() && canWritePrivateFile(candidate)) return candidate;
  }
  return L"";
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
      << L"  HiCT-<version>-windows-x86_64.exe toolbox hictk <hictk arguments...>\n"
      << L"  HiCT-<version>-windows-x86_64.exe toolbox minimap2 <minimap2 arguments...>\n"
      << L"  HiCT-<version>-windows-x86_64.exe toolbox mm2-plus <mm2-plus arguments...>\n"
      << L"  HiCT-<version>-windows-x86_64.exe --hict-extract-only <directory>\n\n"
      << L"DATA_DIR defaults to the directory containing this EXE unless explicitly set.\n"
      << L"The toolbox command forwards arguments to bundled/configured third-party tools\n"
      << L"and prints their project, license, and citation notes before launching them.\n"
      << L"Portable files and temporary files prefer the current user's local application data folder.\n";
}

int extractPayload(const std::wstring &selfPath,
                   const Manifest &manifest,
                   const std::wstring &targetRoot,
                   bool launchAfterExtract) {
  if (launchAfterExtract) {
    createDirectories(dirnameOf(targetRoot));
    CacheLock lock(targetRoot + L".lock");
    const auto appHome = joinPath(targetRoot, utf8ToWide(manifest.appDirName));
    if (readSmallTextFile(joinPath(targetRoot, L".payload.sha256")) == manifest.payloadSha256
        && fileExists(joinPath(appHome, L"HiCT.cmd"))
        && fileExists(joinPath(appHome, L"runtime\\bin\\java.exe"))
        && fileExists(joinPath(appHome, L"lib\\hict.jar"))) {
      return 0;
    }
    if (directoryExists(targetRoot)) {
      throw std::runtime_error("An incomplete payload cache is already present; trying another location.");
    }
    const auto staging = targetRoot + L".partial";
    const auto owner = joinPath(staging, L".hict-extracting");
    if (directoryExists(staging)) {
      const auto attributes = GetFileAttributesW(staging.c_str());
      if ((attributes & FILE_ATTRIBUTE_REPARSE_POINT) || readSmallTextFile(owner) != manifest.payloadSha256) {
        throw std::runtime_error("The staging folder cannot be verified as HiCT-owned; trying another location.");
      }
      removeTreeIfExists(staging);
    }
    if (!CreateDirectoryW(staging.c_str(), nullptr)) {
      throw std::runtime_error("Cannot create a private payload staging folder.");
    }
    try {
      writeSmallTextFile(owner, manifest.payloadSha256);
      const int status = extractPayload(selfPath, manifest, staging, false);
      if (status != 0) {
        removeTreeIfExists(staging);
        return status;
      }
      if (!MoveFileExW(staging.c_str(), targetRoot.c_str(), MOVEFILE_WRITE_THROUGH)) {
        const auto error = GetLastError();
        throw std::runtime_error("Cannot publish the extracted payload (Windows error " + std::to_string(error) + ").");
      }
      DeleteFileW(joinPath(targetRoot, L".hict-extracting").c_str());
      return 0;
    } catch (...) {
      removeTreeIfExists(staging);
      throw;
    }
  }

  createDirectories(targetRoot);
  const auto extractorPath = joinPath(targetRoot, L"7zr.exe");
  const auto archivePath = joinPath(targetRoot, L"payload.7z");
  const auto markerPath = joinPath(targetRoot, L".payload.sha256");
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
  printStartupHeader();

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
  const bool versionOnly = argv != nullptr && argc == 2 && std::wcscmp(argv[1], L"--version") == 0;
  if (argv != nullptr) {
    LocalFree(argv);
  }

  const auto cacheKey = utf8ToWide(shortPayloadCacheKey(manifest));
  const auto localHiCTDir = getLocalHiCTDirectory();
  std::wstring cacheRoot;
  int extractExit = 1;
  for (const auto &candidate : {getEnvironmentVariableValue(L"HICT_PORTABLE_CACHE_ROOT"),
                               localHiCTDir.empty() ? std::wstring{} : joinPath(localHiCTDir, L"payloads"),
                               getWritableCacheRoot(selfPath), getFallbackCacheRoot()}) {
    if (candidate.empty()) continue;
    cacheRoot = joinPath(candidate, cacheKey);
    extractExit = tryExtractPayload(selfPath, manifest, cacheRoot, true);
    if (extractExit == 0 && canLaunchBundledJava(cacheRoot, manifest)) break;
    if (extractExit == 0) extractExit = 1;
  }
  if (extractExit != 0 && argc == 1 && isConsoleOwnedByThisProcess()) {
    const auto chosen = chooseWorkingFolder(L"HiCT could not prepare its portable files. Choose a writable folder to retry.");
    if (!chosen.empty()) {
      cacheRoot = joinPath(joinPath(chosen, L"HiCT\\payloads"), cacheKey);
      extractExit = tryExtractPayload(selfPath, manifest, cacheRoot, true);
      if (extractExit == 0 && !canLaunchBundledJava(cacheRoot, manifest)) extractExit = 1;
    }
  }
  if (extractExit != 0) {
    pauseOnFailureIfNeeded(extractExit);
    return extractExit;
  }

  const auto appHome = joinPath(cacheRoot, utf8ToWide(manifest.appDirName));
  const auto args = buildForwardedArguments();
  const auto java = joinPath(appHome, L"runtime\\bin\\java.exe");
  const auto jar = joinPath(appHome, L"lib\\hict.jar");
  const auto configuredOptions = getEnvironmentVariableValue(L"HICT_JAVA_OPTS");
  std::wstring command = quoteArg(java) + L" -XX:MaxRAMPercentage=50.0";
  const auto jarAndArguments = L" -jar " + quoteArg(jar) + (args.empty() ? L"" : L" " + args);
  if (versionOnly) {
    return runProcess(command + L" " + configuredOptions + jarAndArguments, appHome);
  }

  const auto dataDir = getDataDirectory(selfPath);
  createDirectories(dataDir);
  auto tempDir = selectRuntimeTempDir(dataDir, appHome);
  if (tempDir.empty() && argc == 1 && isConsoleOwnedByThisProcess()) {
    const auto chosen = chooseWorkingFolder(L"HiCT could not prepare its temporary files. Choose a writable working folder to retry.");
    if (!chosen.empty()) {
      const auto candidate = joinPath(chosen, L"HiCT\\runtime");
      if (canWritePrivateFile(candidate)) tempDir = candidate;
    }
  }
  if (tempDir.empty()) {
    throw std::runtime_error("No writable runtime folder was found. Choose a working folder or set HICT_TEMP_DIR.");
  }
  if (!environmentVariableIsDefined(L"PROCESSED_DIR")) {
    const auto processed = joinPath(dataDir, L"processed");
    const auto fallback = joinPath(tempDir, L"processed");
    if (canWritePrivateFile(processed)) SetEnvironmentVariableW(L"PROCESSED_DIR", processed.c_str());
    else if (canWritePrivateFile(fallback)) SetEnvironmentVariableW(L"PROCESSED_DIR", fallback.c_str());
    else throw std::runtime_error("No writable processed-data directory could be prepared.");
  }
  SetEnvironmentVariableW(L"DATA_DIR", dataDir.c_str());
  SetEnvironmentVariableW(L"HICT_PORTABLE_DATA_DIR", dataDir.c_str());
  SetEnvironmentVariableW(L"HICT_PORTABLE_EXE_PATH", selfPath.c_str());
  SetEnvironmentVariableW(L"HICT_PORTABLE_CACHE_DIR", cacheRoot.c_str());
  SetEnvironmentVariableW(L"HICT_TEMP_DIR", tempDir.c_str());
  SetEnvironmentVariableW(L"TMP", tempDir.c_str());
  SetEnvironmentVariableW(L"TEMP", tempDir.c_str());

  SetEnvironmentVariableW(L"HICT_APP_HOME", appHome.c_str());
  SetEnvironmentVariableW(L"HICT_JAR_PATH", jar.c_str());
  defaultEnvironment(L"HICT_BIND_HOST", L"127.0.0.1");
  if (args.empty()) defaultEnvironment(L"HICT_LAUNCHER_MODE", L"gui");
  const auto webRoot = joinPath(appHome, L"webui");
  if (fileExists(joinPath(webRoot, L"index.html"))) defaultEnvironment(L"WEBUI_ROOT", webRoot);
  const auto tools = joinPath(appHome, L"toolchains\\windows_x86_64");
  if (fileExists(joinPath(tools, L"manifest.json"))) defaultEnvironment(L"HICT_TOOLCHAIN_DIR", tools);
  const auto browsers = joinPath(appHome, L"browsers\\windows_x86_64");
  if (directoryExists(browsers)) defaultEnvironment(L"HICT_BROWSER_DIR", browsers);

  command += L" " + quoteArg(L"-Djava.io.tmpdir=" + tempDir) + L" " + configuredOptions + jarAndArguments;
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
