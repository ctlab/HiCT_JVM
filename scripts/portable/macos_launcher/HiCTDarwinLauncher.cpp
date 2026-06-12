#include <cstdlib>
#include <cstdio>
#include <filesystem>
#include <iostream>
#include <limits.h>
#include <mach-o/dyld.h>
#include <string>
#include <system_error>
#include <unistd.h>
#include <vector>

namespace fs = std::filesystem;

static std::string executable_path() {
  uint32_t size = 0;
  _NSGetExecutablePath(nullptr, &size);
  std::string path(size, '\0');
  if (_NSGetExecutablePath(path.data(), &size) != 0) {
    return {};
  }
  char resolved[PATH_MAX];
  if (realpath(path.c_str(), resolved) != nullptr) {
    return resolved;
  }
  path.resize(std::char_traits<char>::length(path.c_str()));
  return path;
}

static std::string parent_dir(const std::string& path) {
  return fs::path(path).parent_path().string();
}

static void set_env_if_unset(const char* key, const std::string& value) {
  if (std::getenv(key) == nullptr) {
    setenv(key, value.c_str(), 1);
  }
}

static void set_env_if_unset_existing_dir(const char* key, const std::vector<std::string>& candidates) {
  if (std::getenv(key) != nullptr) {
    return;
  }
  for (const auto& candidate : candidates) {
    std::error_code ec;
    if (fs::is_directory(candidate, ec)) {
      setenv(key, candidate.c_str(), 1);
      return;
    }
  }
}

static std::string selected_data_dir(const std::string& app_home) {
  if (const char* data_dir = std::getenv("DATA_DIR"); data_dir != nullptr && *data_dir != '\0') {
    return data_dir;
  }
  if (const char* portable_dir = std::getenv("HICT_PORTABLE_DATA_DIR"); portable_dir != nullptr && *portable_dir != '\0') {
    return portable_dir;
  }
  return app_home;
}

static std::string selected_temp_dir(const std::string& data_dir) {
  const std::vector<std::string> candidates = {
    std::getenv("HICT_TEMP_DIR") ? std::getenv("HICT_TEMP_DIR") : "",
    std::getenv("HICT_EXEC_TEMP_DIR") ? std::getenv("HICT_EXEC_TEMP_DIR") : "",
    data_dir + "/tmp",
    std::getenv("TMPDIR") ? std::getenv("TMPDIR") : "",
    "/tmp/hict-" + std::to_string(getuid()) + "/runtime",
  };

  for (const auto& candidate : candidates) {
    if (candidate.empty()) {
      continue;
    }
    std::error_code ec;
    fs::create_directories(candidate, ec);
    if (ec) {
      continue;
    }
    if (access(candidate.c_str(), W_OK | X_OK) == 0) {
      return candidate;
    }
  }
  return data_dir + "/tmp";
}

static int launch_java(const std::string& java_path, const std::vector<std::string>& args) {
  std::vector<char*> argv;
  argv.reserve(args.size() + 2);
  argv.push_back(const_cast<char*>(java_path.c_str()));
  for (const auto& arg : args) {
    argv.push_back(const_cast<char*>(arg.c_str()));
  }
  argv.push_back(nullptr);
  execv(java_path.c_str(), argv.data());
  std::perror("execv");
  return 127;
}

int main(int argc, char* argv[]) {
  const std::string exe_path = executable_path();
  if (exe_path.empty()) {
    std::cerr << "Failed to resolve HiCT launcher path.\n";
    return 1;
  }

  const std::string bin_dir = parent_dir(exe_path);
  const std::string app_home = parent_dir(bin_dir);
  const std::string data_dir = selected_data_dir(app_home);
  const std::string temp_dir = selected_temp_dir(data_dir);

  set_env_if_unset("DATA_DIR", data_dir);
  set_env_if_unset("HICT_APP_HOME", app_home);
  set_env_if_unset("HICT_JAR_PATH", app_home + "/lib/hict.jar");
  set_env_if_unset("WEBUI_ROOT", app_home + "/webui");
  set_env_if_unset_existing_dir("HICT_TOOLCHAIN_DIR", {
    app_home + "/toolchains/darwin_arm64",
    app_home + "/toolchains/darwin_x86_64"
  });
  set_env_if_unset_existing_dir("HICT_BROWSER_DIR", {
    app_home + "/browsers/darwin_arm64",
    app_home + "/browsers/darwin_x86_64"
  });
  set_env_if_unset("HICT_TEMP_DIR", temp_dir);
  set_env_if_unset("TMPDIR", temp_dir);
  set_env_if_unset("TMP", temp_dir);
  set_env_if_unset("TEMP", temp_dir);
  set_env_if_unset("HICT_BIND_HOST", "127.0.0.1");
  if (argc == 1) {
    set_env_if_unset("HICT_LAUNCHER_MODE", "gui");
  }

  const std::string java_path = app_home + "/runtime/bin/java";
  if (access(java_path.c_str(), X_OK) != 0) {
    std::cerr << "Missing embedded Java runtime: " << java_path << "\n";
    return 1;
  }

  std::vector<std::string> trailing_args;
  for (int i = 1; i < argc; ++i) {
    trailing_args.emplace_back(argv[i]);
  }

  std::vector<std::string> final_args;
  final_args.reserve(3 + trailing_args.size());
  final_args.emplace_back("-Djava.io.tmpdir=" + temp_dir);
  final_args.emplace_back("-jar");
  final_args.emplace_back(app_home + "/lib/hict.jar");
  for (const auto& arg : trailing_args) {
    final_args.emplace_back(arg);
  }

  return launch_java(java_path, final_args);
}
