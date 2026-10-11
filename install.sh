#!/usr/bin/env bash
# Quay's zero-configuration installer. It bootstraps an isolated uv tool
# environment, so it never writes into an OS-managed Python installation.
set -euo pipefail

readonly APP_NAME="Quay"
# Override only for release-candidate smoke tests; public installs use PyPI.
readonly PACKAGE_NAME="${QUAY_PACKAGE_NAME:-oquay}"
readonly PYTHON_VERSION="3.11"
readonly BIN_DIR="${XDG_BIN_HOME:-$HOME/.local/bin}"
UV_BIN=""

ensure_shell_path() {
    local shell_name rc marker
    shell_name="${SHELL##*/}"
    marker="# Added by ${APP_NAME} installer"
    case "${shell_name:-bash}" in
        zsh) rc="$HOME/.zshrc" ;;
        fish)
            rc="$HOME/.config/fish/conf.d/quay.fish"
            mkdir -p "$(dirname "$rc")"
            touch "$rc"
            if ! grep -Fqx "$marker" "$rc"; then
                printf '\n%s\nfish_add_path %s\n' "$marker" "$BIN_DIR" >> "$rc"
            fi
            return
            ;;
        *) rc="$HOME/.bashrc" ;;
    esac
    mkdir -p "$(dirname "$rc")"
    touch "$rc"
    if ! grep -Fqx "$marker" "$rc"; then
        printf '\n%s\nexport PATH="%s:$PATH"\n' "$marker" "$BIN_DIR" >> "$rc"
    fi
}

bootstrap_uv() {
    if command -v uv >/dev/null 2>&1; then
        UV_BIN="$(command -v uv)"
        return
    fi
    if [ -x "$BIN_DIR/uv" ]; then
        UV_BIN="$BIN_DIR/uv"
        return
    fi
    if ! command -v curl >/dev/null 2>&1; then
        printf '%s\n' "${APP_NAME} needs curl for this one-command bootstrap." >&2
        printf '%s\n' "Install curl, then rerun the same command." >&2
        exit 1
    fi

    mkdir -p "$BIN_DIR"
    printf '%s\n' "Bootstrapping uv and an isolated Python runtime..."
    UV_INSTALL_DIR="$BIN_DIR" curl --proto '=https' --tlsv1.2 -LsSf \
        https://astral.sh/uv/install.sh | sh
    if [ ! -x "$BIN_DIR/uv" ]; then
        printf '%s\n' "uv bootstrap did not create $BIN_DIR/uv" >&2
        exit 1
    fi
    UV_BIN="$BIN_DIR/uv"
}

main() {
    bootstrap_uv
    mkdir -p "$BIN_DIR"

    # --managed-python allows a fresh machine with no Python installation to
    # receive a compatible runtime automatically.
    UV_TOOL_BIN_DIR="$BIN_DIR" "$UV_BIN" tool install --upgrade \
        --python "$PYTHON_VERSION" --managed-python "$PACKAGE_NAME"
    ensure_shell_path

    printf '\n%s\n' "${APP_NAME} installed successfully."
    "$BIN_DIR/quay" --version
    printf '%s\n' "Run now: $BIN_DIR/quay init"
    printf '%s\n' "Future terminals can use: quay"
}

main "$@"
