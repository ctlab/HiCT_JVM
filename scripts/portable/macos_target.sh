# Shared by Darwin toolchain, browser and portable-package entrypoints.
# MACOSX_DEPLOYMENT_TARGET is the compiler standard; retain the older HiCT alias.
if [[ -n "${MACOSX_DEPLOYMENT_TARGET:-}" && -n "${MACOS_DEPLOYMENT_TARGET:-}" &&
      "${MACOSX_DEPLOYMENT_TARGET}" != "${MACOS_DEPLOYMENT_TARGET}" ]]; then
  echo "Conflicting MACOSX_DEPLOYMENT_TARGET and MACOS_DEPLOYMENT_TARGET." >&2
  return 1
fi
export MACOSX_DEPLOYMENT_TARGET="${MACOSX_DEPLOYMENT_TARGET:-${MACOS_DEPLOYMENT_TARGET:-12.3}}"
if [[ ! "${MACOSX_DEPLOYMENT_TARGET}" =~ ^([0-9]+)\.([0-9]+)(\.[0-9]+)?$ ]] ||
   (( 10#${BASH_REMATCH[1]} < 12 || (10#${BASH_REMATCH[1]} == 12 && 10#${BASH_REMATCH[2]} < 3) )); then
  echo "HiCT's complete macOS bundle requires a deployment target of 12.3 or newer." >&2
  return 1
fi
export MACOS_DEPLOYMENT_TARGET="${MACOSX_DEPLOYMENT_TARGET}"
