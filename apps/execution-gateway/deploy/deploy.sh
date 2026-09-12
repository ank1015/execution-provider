#!/bin/sh
set -eu

: "${GATEWAY_IMAGE:?GATEWAY_IMAGE is required}"
: "${GATEWAY_HOST:?GATEWAY_HOST is required}"
: "${GCP_PROJECT_NUMBER:?GCP_PROJECT_NUMBER is required}"

cd /opt/execution-gateway/release
umask 077

metadata_header='Metadata-Flavor: Google'
token_json="$(curl -fsS -H "$metadata_header" \
  http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token)"
access_token="$(printf '%s' "$token_json" | jq -r .access_token)"

read_secret() {
  curl -fsS \
    -H "Authorization: Bearer $access_token" \
    "https://secretmanager.googleapis.com/v1/projects/$GCP_PROJECT_NUMBER/secrets/$1/versions/latest:access" \
    | jq -r .payload.data \
    | base64 -d
}

database_url="$(read_secret execution-gateway-2-database-url)"
admin_api_key="$(read_secret execution-gateway-2-admin-api-key)"
encryption_key="$(read_secret execution-gateway-2-encryption-key)"

{
  printf 'DATABASE_URL=%s\n' "$database_url"
  printf 'ADMIN_API_KEY=%s\n' "$admin_api_key"
  printf 'ENCRYPTION_KEY=%s\n' "$encryption_key"
  printf 'LISTEN_ADDR=0.0.0.0:3000\n'
  printf 'WEBHOOK_ALLOWED_ORIGINS=\n'
  printf 'REQUEST_RETENTION_DAYS=7\n'
} > runtime.env.new
chmod 0600 runtime.env.new
mv runtime.env.new runtime.env

write_image_env() {
  printf 'GATEWAY_IMAGE=%s\n' "$1" > .env.new
  chmod 0600 .env.new
  mv .env.new .env
}

registry="${GATEWAY_IMAGE%%/*}"
printf '%s' "$access_token" \
  | docker login -u oauth2accesstoken --password-stdin "https://$registry"
trap 'docker logout "$registry" >/dev/null 2>&1 || true' EXIT

export GATEWAY_IMAGE
docker-compose -f compose.production.yml config >/dev/null
docker-compose -f compose.production.yml pull gateway
docker-compose -f compose.production.yml run --rm migrate
docker run --rm \
  -v "$PWD/Caddyfile:/etc/caddy/Caddyfile:ro" \
  caddy:2.10-alpine \
  caddy validate --config /etc/caddy/Caddyfile --adapter caddyfile

current_container="$(docker-compose -f compose.production.yml ps -q gateway)"
previous_image=''
if [ -n "$current_container" ]; then
  previous_image="$(docker inspect --format '{{.Config.Image}}' "$current_container")"
fi

write_image_env "$GATEWAY_IMAGE"
docker-compose -f compose.production.yml up -d --no-deps --force-recreate gateway

caddy_container="$(docker-compose -f compose.production.yml ps -q caddy)"
if [ -n "$caddy_container" ]; then
  docker exec "$caddy_container" caddy reload --config /etc/caddy/Caddyfile --adapter caddyfile
else
  docker-compose -f compose.production.yml up -d --no-deps caddy
fi

attempt=0
until curl -fsS --resolve "$GATEWAY_HOST:443:127.0.0.1" \
  "https://$GATEWAY_HOST/readyz" >/dev/null; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 30 ]; then
    if [ -n "$previous_image" ]; then
      echo "The updated gateway did not become ready; restoring $previous_image." >&2
      write_image_env "$previous_image"
      GATEWAY_IMAGE="$previous_image" \
        docker-compose -f compose.production.yml up -d --no-deps --force-recreate gateway
    fi
    exit 1
  fi
  sleep 2
done

docker image prune -f >/dev/null
