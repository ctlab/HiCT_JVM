#!/usr/bin/env bash
set -euo pipefail

repo="${HICT_JHDF5_REPO:-AxisAlexNT/jhdf5-with-plugins-configuration-snapshot}"
ref="${HICT_JHDF5_REF:-${GITHUB_REF_NAME:-jhdf5-with-plugins-configuration-snapshot}}"
jar_name="${HICT_JHDF5_JAR_NAME:-sis-jhdf5-19.04.1-slim.jar}"
out="${HICT_JHDF5_LOCAL_JAR:-src/main/resources/libs/${jar_name}}"
natives_archive_name="${HICT_JHDF5_NATIVES_ARCHIVE_NAME:-sis-jhdf5-19.04.1-natives.tar.gz}"
natives_archive_out="${HICT_JHDF5_NATIVES_ARCHIVE:-src/main/resources/libs/${natives_archive_name}}"
fallback_jar_name="${HICT_JHDF5_FALLBACK_JAR_NAME:-sis-jhdf5-19.04.1.jar}"
fallback_jar_out="${HICT_JHDF5_FALLBACK_LOCAL_JAR:-src/main/resources/libs/${fallback_jar_name}}"
mode="${HICT_JHDF5_SOURCE_MODE:-release}"
release_tag="${HICT_JHDF5_RELEASE_TAG:-release-artifacts}"
artifact_name="${HICT_JHDF5_ARTIFACT_NAME:-jhdf5-packaged-jar}"
strict_snapshot="${HICT_REQUIRE_SNAPSHOT_JHDF5:-0}"

emit_env() {
  if [[ -n "${GITHUB_ENV:-}" ]]; then
    printf '%s\n' "$@" >> "${GITHUB_ENV}"
  fi
}

use_maven_fallback() {
  local reason="$1"
  echo "::warning::Falling back to Maven-provided cisd:jhdf5:19.04.1 because ${reason}"
  emit_env \
    "HICT_JHDF5_SOURCE_MODE=maven" \
    "HICT_USE_MAVEN_JHDF5=1" \
    "HICT_REQUIRE_BUNDLED_JHDF5=0"
  exit 0
}

required_patterns_for_runner() {
  local os="${RUNNER_OS:-$(uname -s)}"
  local arch="${RUNNER_ARCH:-$(uname -m)}"
  case "${os}:${arch}" in
    Windows:X64|Windows:AMD64|MINGW*:*)
      printf '%s\n' '^native/jhdf5/amd64-Windows/jhdf5\.dll$'
      ;;
    macOS:ARM64|Darwin:arm64|Darwin:aarch64)
      printf '%s\n' '^(resources/libs/(osx_arm64|macos_arm64|darwin_arm64)/libhdf5\.dylib|native/jhdf5/aarch64-Mac OS X/libhdf5\.[0-9.]+\.dylib|libs/native/jhdf5/aarch64-Mac OS X/libhdf5\.[0-9.]+\.dylib)$'
      ;;
    macOS:X64|macOS:AMD64|Darwin:x86_64)
      printf '%s\n' '^(resources/libs/(osx_64|macos_64|darwin_x86_64)/libhdf5\.dylib|native/jhdf5/x86_64-Mac OS X/libhdf5\.[0-9.]+\.dylib|libs/native/jhdf5/x86_64-Mac OS X/libhdf5\.[0-9.]+\.dylib)$'
      ;;
    Linux:ARM64|Linux:AARCH64|Linux:aarch64)
      printf '%s\n' '^native/jhdf5/(aarch64|arm64)-Linux/libjhdf5\.so$'
      ;;
    Linux:*|*)
      printf '%s\n' '^native/jhdf5/amd64-Linux/libjhdf5\.so$'
      ;;
  esac
}

case "${mode}" in
  maven|maven-central|published)
    echo "HICT_JHDF5_SOURCE_MODE=${mode}; using Maven-provided cisd:jhdf5:19.04.1."
    emit_env \
      "HICT_JHDF5_SOURCE_MODE=maven" \
      "HICT_USE_MAVEN_JHDF5=1" \
      "HICT_REQUIRE_BUNDLED_JHDF5=0"
    exit 0
    ;;
esac

mkdir -p "$(dirname "${out}")"
mkdir -p "$(dirname "${natives_archive_out}")"
resolve_error=""

if [[ -f "${out}" ]]; then
  echo "Using existing JHDF5 jar: ${out}"
else
  if ! command -v gh >/dev/null 2>&1; then
    if [[ "${strict_snapshot}" == "1" || "${mode}" == "local" ]]; then
      echo "GitHub CLI (gh) is required to resolve ${repo} JHDF5 jar." >&2
      exit 1
    fi
    use_maven_fallback "GitHub CLI (gh) is unavailable"
  fi
  tmp="$(mktemp -d)"
  trap 'rm -rf "${tmp}"' EXIT
  set +e
  case "${mode}" in
    local)
      resolve_error="HICT_JHDF5_SOURCE_MODE=local but ${out} does not exist."
      ;;
    release)
      if [[ "${release_tag}" == "latest" ]]; then
        gh release download --repo "${repo}" --pattern "${jar_name}" --dir "${tmp}"
      else
        gh release download "${release_tag}" --repo "${repo}" --pattern "${jar_name}" --dir "${tmp}"
      fi
      rc=$?
      if [[ ${rc} -ne 0 && "${fallback_jar_name}" != "${jar_name}" ]]; then
        if [[ "${release_tag}" == "latest" ]]; then
          gh release download --repo "${repo}" --pattern "${fallback_jar_name}" --dir "${tmp}"
        else
          gh release download "${release_tag}" --repo "${repo}" --pattern "${fallback_jar_name}" --dir "${tmp}"
        fi
        rc=$?
      fi
      if [[ ${rc} -ne 0 ]]; then resolve_error="release ${release_tag} in ${repo} has no downloadable ${jar_name} or ${fallback_jar_name} payload"; fi
      ;;
    artifact|branch|workflow|auto)
      run_id="$(gh run list --repo "${repo}" --branch "${ref}" --workflow build-native.yml --status success --limit 1 --json databaseId --jq '.[0].databaseId' 2>/dev/null)"
      rc=$?
      if [[ ${rc} -ne 0 || -z "${run_id}" || "${run_id}" == "null" ]]; then
        resolve_error="no successful build-native.yml run exists in ${repo} on branch/ref ${ref}"
      else
        gh run download "${run_id}" --repo "${repo}" --name "${artifact_name}" --dir "${tmp}"
        rc=$?
        if [[ ${rc} -ne 0 ]]; then resolve_error="artifact ${artifact_name} is missing from successful run ${run_id}"; fi
      fi
      ;;
    *)
      echo "Unsupported HICT_JHDF5_SOURCE_MODE=${mode}; use artifact, release, local, or maven." >&2
      exit 1
      ;;
  esac
  set -e
  if [[ -n "${resolve_error}" ]]; then
    if [[ "${strict_snapshot}" == "1" || "${mode}" == "local" ]]; then
      echo "${resolve_error}" >&2
      exit 1
    fi
    use_maven_fallback "${resolve_error}"
  fi
  found="$(find "${tmp}" -type f -name "${jar_name}" | sort | sed -n '1p')"
  if [[ -z "${found}" && "${fallback_jar_name}" != "${jar_name}" ]]; then
    found="$(find "${tmp}" -type f -name "${fallback_jar_name}" | sort | sed -n '1p')"
    if [[ -n "${found}" ]]; then
      out="${fallback_jar_out}"
      echo "::warning::Downloaded payload does not contain ${jar_name}; using fallback ${fallback_jar_name}."
    fi
  fi
  if [[ -z "${found}" ]]; then
    if [[ "${strict_snapshot}" == "1" ]]; then
      echo "Downloaded payload does not contain ${jar_name}." >&2
      find "${tmp}" -maxdepth 4 -type f >&2 || true
      exit 1
    fi
    use_maven_fallback "downloaded ${repo} artifact does not contain ${jar_name}"
  fi
  cp "${found}" "${out}"
fi

if [[ ! -f "${natives_archive_out}" ]]; then
  if [[ "${mode}" == "release" ]]; then
    tmp_archive="$(mktemp -d)"
    trap 'rm -rf "${tmp_archive}"' EXIT
    set +e
    if [[ "${release_tag}" == "latest" ]]; then
      gh release download --repo "${repo}" --pattern "${natives_archive_name}" --dir "${tmp_archive}"
    else
      gh release download "${release_tag}" --repo "${repo}" --pattern "${natives_archive_name}" --dir "${tmp_archive}"
    fi
    rc=$?
    set -e
    if [[ ${rc} -eq 0 ]]; then
      found_archive="$(find "${tmp_archive}" -type f -name "${natives_archive_name}" | sort | sed -n '1p')"
      [[ -z "${found_archive}" ]] || cp "${found_archive}" "${natives_archive_out}"
    fi
  elif [[ -d "${tmp:-}" ]]; then
    found_archive="$(find "${tmp}" -type f -name "${natives_archive_name}" | sort | sed -n '1p')"
    [[ -z "${found_archive}" ]] || cp "${found_archive}" "${natives_archive_out}"
  fi
fi

build_entries() {
  local jar_path="$1"
  local combined
  combined="$(jar tf "${jar_path}")"
  if [[ -f "${natives_archive_out}" ]]; then
    combined+=$'\n'
    combined+="$(tar -tzf "${natives_archive_out}" | sed 's#^#archive:#')"
  fi
  printf '%s\n' "${combined}"
}

validate_required_entries() {
  local jar_path="$1"
  local entries="$2"
  local local_missing=0
  while IFS= read -r pattern; do
    archive_pattern="^archive:${pattern#^}"
    if ! grep -Eq "${pattern}|${archive_pattern}" <<<"${entries}"; then
      echo "::warning::JHDF5 snapshot jar ${jar_path} lacks required native entry matching ${pattern} for ${RUNNER_OS:-unknown}/${RUNNER_ARCH:-unknown}."
      local_missing=1
    fi
  done < <(required_patterns_for_runner)
  return "${local_missing}"
}

entries="$(build_entries "${out}")"
missing=0
validate_required_entries "${out}" "${entries}" || missing=1

if [[ ${missing} -ne 0 ]]; then
  if [[ "${fallback_jar_name}" != "$(basename "${out}")" && -f "${fallback_jar_out}" ]]; then
    fallback_entries="$(build_entries "${fallback_jar_out}")"
    if validate_required_entries "${fallback_jar_out}" "${fallback_entries}"; then
      echo "::warning::Using fallback JHDF5 jar ${fallback_jar_out} because ${out} has no ready native payload for this runner."
      out="${fallback_jar_out}"
      missing=0
    fi
  fi
fi

if [[ ${missing} -ne 0 ]]; then
  if [[ "${strict_snapshot}" == "1" ]]; then
    echo "Required snapshot JHDF5 native payload is missing and HICT_REQUIRE_SNAPSHOT_JHDF5=1." >&2
    exit 1
  fi
  rm -f "${out}"
  use_maven_fallback "snapshot JHDF5 artifact has no ready native payload for ${RUNNER_OS:-this OS}/${RUNNER_ARCH:-this arch}"
fi

emit_env \
  "HICT_JHDF5_SOURCE_MODE=${mode}" \
  "HICT_JHDF5_LOCAL_JAR=${out}" \
  "HICT_JHDF5_NATIVES_ARCHIVE=${natives_archive_out}" \
  "HICT_REQUIRE_BUNDLED_JHDF5=1"
echo "Resolved JHDF5 jar: ${out}"
if [[ -f "${natives_archive_out}" ]]; then
  echo "Resolved JHDF5 native archive: ${natives_archive_out}"
else
  echo "::warning::No split JHDF5 native archive was resolved; runtime will rely on jar-embedded natives."
fi
