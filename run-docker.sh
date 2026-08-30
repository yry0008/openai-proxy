#!/usr/bin/env bash

set -euo pipefail

IMAGE_NAME="${IMAGE_NAME:-openai-proxy:video-process}"
CONTAINER_NAME="${CONTAINER_NAME:-openai-proxy}"
HOST_PORT="${HOST_PORT:-3280}"
TARGET_SERVER="${TARGET_SERVER:-http://10.86.7.230:8088}"

# Replace only the container created by this script so rerunning it is idempotent.
docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true

env_args=()
if [[ -f .env ]]; then
    env_args+=(--env-file .env)
fi
env_args+=(--env "TARGET_SERVER=$TARGET_SERVER")

# Explicit shell variables take precedence over values from .env.
for variable in API_KEY MODEL_NAME REASONING_TYPE WORKERS MAX_RETRIES; do
    if [[ -n "${!variable+x}" ]]; then
        env_args+=(--env "$variable=${!variable}")
    fi
done

docker run -d \
    --name "$CONTAINER_NAME" \
    --restart unless-stopped \
    --publish "$HOST_PORT:3280" \
    "${env_args[@]}" \
    "$IMAGE_NAME"

echo "Container started: $CONTAINER_NAME"
echo "Proxy URL: http://127.0.0.1:$HOST_PORT"
echo "Upstream: $TARGET_SERVER"
echo "Logs: docker logs -f $CONTAINER_NAME"
