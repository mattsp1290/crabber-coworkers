#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
arch=${1:-arm64}
case "$arch" in
  arm64) triple=aarch64-unknown-linux-gnu ;;
  amd64) triple=x86_64-unknown-linux-gnu ;;
  *) echo 'usage: scripts/build-worker-image.sh [arm64|amd64]' >&2; exit 2 ;;
esac
rustup target add "$triple"
cargo zigbuild --release --locked -p crabber-coworkers --target "$triple.2.36" --target-dir target/worker-build
context="target/worker-image/$arch"
mkdir -p "$context"
install -m 0755 "target/worker-build/$triple/release/crabber-coworkers" "$context/crabber-coworkers"
install -m 0644 "${SSL_CERT_FILE:-/etc/ssl/cert.pem}" "$context/ca-certificates.crt"
docker build --platform "linux/$arch" --file deploy/worker.Dockerfile --tag "crabber-coworkers-worker:$arch" "$context"
