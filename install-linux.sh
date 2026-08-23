#!/usr/bin/env bash
set -Eeuo pipefail

readonly REPO_DEFAULT="https://github.com/AnacondaKC/Aniu.git"
readonly BRANCH_DEFAULT="main"
readonly HOME_DIR="${HOME:-}"
readonly INSTALL_DIR_DEFAULT="${HOME_DIR}/Aniu"

fail() {
  printf 'Aniu installer: %s\n' "$*" >&2
  exit 1
}

usage() {
  cat <<'EOF'
Usage: install-linux.sh [options]

Clone or update Aniu, install its dependencies, and start the service.

Options:
  --dir PATH       Installation directory (default: $HOME/Aniu)
  --branch NAME    Git branch (default: main)
  --repo URL       Git repository URL
  -h, --help       Show this help

Environment overrides:
  ANIU_INSTALL_DIR, ANIU_BRANCH, ANIU_REPO_URL

Examples:
  curl -fsSL https://raw.githubusercontent.com/AnacondaKC/Aniu/main/install-linux.sh | bash
  curl -fsSL https://raw.githubusercontent.com/AnacondaKC/Aniu/main/install-linux.sh | bash -s -- --dir "$HOME/aniu"
EOF
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || fail "required command not found: $1"
}

canonical_repo_url() {
  local value="${1%/}"
  case "$value" in
    "https://github.com/AnacondaKC/Aniu"|"https://github.com/AnacondaKC/Aniu.git"|"git@github.com:AnacondaKC/Aniu.git"|"ssh://git@github.com/AnacondaKC/Aniu.git")
      printf '%s\n' "$REPO_DEFAULT"
      ;;
    *)
      printf '%s\n' "$value"
      ;;
  esac
}

resolve_path() {
  local value="$1"
  local parent
  local name

  case "$value" in
    "~") value="$HOME_DIR" ;;
    "~/"*) value="$HOME_DIR/${value#~/}" ;;
  esac

  if [[ "$value" != /* ]]; then
    value="$PWD/$value"
  fi

  parent="$(dirname -- "$value")"
  name="$(basename -- "$value")"
  [ "$name" != "/" ] || fail "installation directory must not be filesystem root"
  mkdir -p -- "$parent"
  parent="$(CDPATH= cd -- "$parent" && pwd)"
  printf '%s/%s\n' "$parent" "$name"
}

[ -n "$HOME_DIR" ] || fail 'HOME is not set'

repo_url="${ANIU_REPO_URL:-$REPO_DEFAULT}"
branch="${ANIU_BRANCH:-$BRANCH_DEFAULT}"
install_dir="${ANIU_INSTALL_DIR:-$INSTALL_DIR_DEFAULT}"

while [ "$#" -gt 0 ]; do
  case "$1" in
    --dir)
      [ "$#" -ge 2 ] || fail "--dir requires a path"
      install_dir="$2"
      shift 2
      ;;
    --branch)
      [ "$#" -ge 2 ] || fail "--branch requires a name"
      branch="$2"
      shift 2
      ;;
    --repo)
      [ "$#" -ge 2 ] || fail "--repo requires a URL"
      repo_url="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      fail "unknown option: $1 (use --help for usage)"
      ;;
  esac
done

[ "$(uname -s)" = "Linux" ] || fail "this installer supports Linux only"
[ -n "$repo_url" ] || fail "repository URL must not be empty"
[ -n "$branch" ] || fail "branch must not be empty"
[ -n "$install_dir" ] || fail "installation directory must not be empty"

require_command git
require_command node
require_command npm
require_command python3

install_dir="$(resolve_path "$install_dir")"

printf 'Aniu installation directory: %s\n' "$install_dir"
printf 'Aniu repository: %s (%s)\n' "$repo_url" "$branch"

if [ -e "$install_dir" ]; then
  [ -d "$install_dir/.git" ] || fail "$install_dir exists but is not an Aniu Git checkout"

  current_remote="$(git -C "$install_dir" remote get-url origin 2>/dev/null || true)"
  [ -n "$current_remote" ] || fail "$install_dir has no origin remote"
  [ "$(canonical_repo_url "$current_remote")" = "$(canonical_repo_url "$repo_url")" ] || fail "$install_dir points to a different repository: $current_remote"

  [ -z "$(git -C "$install_dir" status --porcelain)" ] || fail "$install_dir has local changes; commit or stash them before updating"

  printf '%s\n' 'Updating the existing checkout (fast-forward only)'
  git -C "$install_dir" fetch origin "$branch"
  git -C "$install_dir" pull --ff-only origin "$branch"
else
  printf '%s\n' 'Cloning Aniu'
  git clone --depth 1 --branch "$branch" "$repo_url" "$install_dir"
fi

[ -x "$install_dir/install.sh" ] || fail "install.sh is missing or not executable in $install_dir"

printf '%s\n' 'Installing dependencies and starting Aniu'
cd "$install_dir"
exec ./install.sh
