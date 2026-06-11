#!/usr/bin/env bash
set -euo pipefail

repo="${HICT_JHDF5_REPO:-AxisAlexNT/jhdf5-with-plugins-configuration-snapshot}"
ref="${HICT_JHDF5_REF:-${GITHUB_REF_NAME:-master}}"
jar_name="${HICT_JHDF5_JAR_NAME:-sis-jhdf5-19.04.1.jar}"
out="${HICT_JHDF5_LOCAL_JAR:-src/main/resources/libs/${jar_name}}"
mode="${HICT_JHDF5_SOURCE_MODE:-artifact}"
release_tag="${HICT_JHDF5_RELEASE_TAG:-latest}"
artifact_name="${HICT_JHDF5_ARTIFACT_NAME:-jhdf5-packaged-jar}"

mkdir -p "$(dirname "${out}")"

if [[ -f "${out}" ]]; then
  echo "Using existing JHDF5 jar: ${out}"
else
  if ! command -v gh >/dev/null 2>&1; then
    echo "GitHub CLI (gh) is required to resolve ${repo} JHDF5 jar." >&2
    exit 1
  fi
  tmp="$(mktemp -d)"
  trap 'rm -rf "${tmp}"' EXIT
  case "${mode}" in
    local)
      echo "HICT_JHDF5_SOURCE_MODE=local but ${out} does not exist." >&2
      exit 1
      ;;
    release)
      if [[ "${release_tag}" == "latest" ]]; then
        gh release download --repo "${repo}" --pattern "${jar_name}" --dir "${tmp}"
      else
        gh release download "${release_tag}" --repo "${repo}" --pattern "${jar_name}" --dir "${tmp}"
      fi
      ;;
    artifact|branch|workflow)
      run_id="$(gh run list --repo "${repo}" --branch "${ref}" --workflow build-native.yml --status success --limit 1 --json databaseId --jq '.[0].databaseId')"
      if [[ -z "${run_id}" || "${run_id}" == "null" ]]; then
        echo "No successful build-native.yml run found in ${repo} on branch/ref ${ref}." >&2
        exit 1
      fi
      gh run download "${run_id}" --repo "${repo}" --name "${artifact_name}" --dir "${tmp}"
      ;;
    *)
      echo "Unsupported HICT_JHDF5_SOURCE_MODE=${mode}; use artifact, release, or local." >&2
      exit 1
      ;;
  esac
  found="$(find "${tmp}" -type f -name "${jar_name}" | sort | head -n 1)"
  if [[ -z "${found}" ]]; then
    echo "Downloaded payload does not contain ${jar_name}." >&2
    find "${tmp}" -type f -maxdepth 4 >&2 || true
    exit 1
  fi
  cp "${found}" "${out}"
fi

jar tf "${out}" | grep -q '^native/jhdf5/amd64-Windows/jhdf5\.dll$'
jar tf "${out}" | grep -Eq '^resources/libs/(osx_arm64|macos_arm64|darwin_arm64)/libhdf5\.dylib$'
jar tf "${out}" | grep -Eq '^resources/libs/(osx_64|macos_64|darwin_x86_64)/libhdf5\.dylib$'
echo "HICT_JHDF5_LOCAL_JAR=${out}" >> "${GITHUB_ENV:-/dev/null}" || true
echo "Resolved JHDF5 jar: ${out}"
