#!/usr/bin/env bash
set -euo pipefail
# Disposable development fixture. Do not reuse these public credentials in production.
repo_dir="$(cd "$(dirname "$0")/.." && pwd -P)"
state_dir="$repo_dir/build/garage"
mkdir -p "$state_dir/storage"
cat > "$state_dir/garage.toml" <<'CONFIG'
metadata_dir = "/storage/meta"
data_dir = "/storage/data"
db_engine = "sqlite"
replication_factor = 1
rpc_bind_addr = "[::]:3901"
rpc_public_addr = "127.0.0.1:3901"
rpc_secret = "1111111111111111111111111111111111111111111111111111111111111111"
[s3_api]
s3_region = "garage"
api_bind_addr = "[::]:3900"
root_domain = ".s3.garage.localhost"
CONFIG
container_id=$(docker run --rm -d --name "oss-s3-garage-${GARAGE_RUN_ID:-ci}" \
  -p "127.0.0.1:${GARAGE_PORT:-19390}:3900" \
  -v "$state_dir/garage.toml:/etc/garage.toml:ro" -v "$state_dir/storage:/storage" \
  -e GARAGE_DEFAULT_ACCESS_KEY=GK11111111111111111111111111111111 \
  -e GARAGE_DEFAULT_SECRET_KEY=synthetic-local-test-secret-not-for-production \
  -e GARAGE_DEFAULT_BUCKET=stream-tests \
  dxflrs/garage:v2.4.1@sha256:9c96caa2612d3411acc5b0e6701fb238dbfba33e533a6d7d3d811a4b12d0d020 \
  /garage server --single-node --default-bucket)
printf '%s\n' "$container_id" > "$state_dir/container-id"
for attempt in $(seq 1 30); do
  if curl -s -o /dev/null "http://127.0.0.1:${GARAGE_PORT:-19390}/"; then
    printf 'Garage fixture ready: %s\n' "$container_id"
    exit 0
  fi
  sleep 1
done
docker logs "$container_id"
exit 1
