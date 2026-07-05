#!/usr/bin/env bash
set -euo pipefail

VERSION="${IBC_VERSION:-3.24.1}"
INSTALL_DIR="${IBC_INSTALL_DIR:-/home/ubuntu/apps/ibc}"
TMP_DIR="$(mktemp -d)"

cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

ZIP_NAME="IBCLinux-${VERSION}.zip"
URL="https://github.com/IbcAlpha/IBC/releases/download/${VERSION}/${ZIP_NAME}"

mkdir -p "$(dirname "$INSTALL_DIR")"
cd "$TMP_DIR"
curl -fsSLO "$URL"
mkdir extract
unzip -q "$ZIP_NAME" -d extract

rm -rf "$INSTALL_DIR"
mkdir -p "$INSTALL_DIR"
cp -a extract/. "$INSTALL_DIR/"
chmod +x "$INSTALL_DIR"/*.sh "$INSTALL_DIR"/scripts/*.sh

echo "Installed IBC $VERSION to $INSTALL_DIR"
