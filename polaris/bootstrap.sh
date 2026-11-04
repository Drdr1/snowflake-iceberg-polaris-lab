#!/usr/bin/env bash
# Create the lab catalog in Polaris plus two access paths:
#   - root (service_admin)        -> full control, used by Spark and the tests
#   - snowflake_reader principal  -> read-only, the identity Snowflake uses
#
# Works for both setups:
#   local MinIO (default)  : S3_ENDPOINT=http://localhost:9000, internal http://minio:9000
#   real AWS S3            : set STORAGE_ROLE_ARN (and optionally STORAGE_EXTERNAL_ID),
#                            leave S3_ENDPOINT empty. See docs/RUNBOOK.md step 3.
#
# Safe to re-run: "already exists" (409) is treated as success.
set -euo pipefail

POLARIS_URL="${POLARIS_URL:-http://localhost:8181}"
REALM="${POLARIS_REALM:-POLARIS}"
ROOT_CREDENTIAL="${POLARIS_ROOT_CREDENTIAL:-root:s3cr3t}"
CATALOG="${POLARIS_CATALOG:-lab_catalog}"
BASE_LOCATION="${BASE_LOCATION:-s3://lakehouse/lab_catalog}"
AWS_REGION="${AWS_REGION:-us-west-2}"
S3_ENDPOINT="${S3_ENDPOINT-http://localhost:9000}"
S3_ENDPOINT_INTERNAL="${S3_ENDPOINT_INTERNAL-http://minio:9000}"
STORAGE_ROLE_ARN="${STORAGE_ROLE_ARN:-}"
STORAGE_EXTERNAL_ID="${STORAGE_EXTERNAL_ID:-}"
READER_ENV_FILE="${READER_ENV_FILE:-.polaris-reader.env}"

need() { command -v "$1" >/dev/null || { echo "missing tool: $1" >&2; exit 1; }; }
need curl; need jq

api() {  # api METHOD PATH [JSON]  -> prints body; 409 is OK
  local method=$1 path=$2 body=${3:-} out code
  out=$(mktemp)
  code=$(curl -sS -o "$out" -w '%{http_code}' -X "$method" \
    -H "Authorization: Bearer $TOKEN" -H "Polaris-Realm: $REALM" \
    -H 'Content-Type: application/json' -H 'Accept: application/json' \
    ${body:+-d "$body"} "$POLARIS_URL/api/management/v1$path")
  if [[ $code == 409 ]]; then echo "  (exists) $method $path" >&2; rm -f "$out"; return 0; fi
  if [[ $code -ge 300 ]]; then echo "FAILED $code $method $path: $(cat "$out")" >&2; rm -f "$out"; exit 1; fi
  cat "$out"; rm -f "$out"
}

echo "Getting root token from $POLARIS_URL ..."
TOKEN=$(curl -sS --fail-with-body "$POLARIS_URL/api/catalog/v1/oauth/tokens" \
  --user "$ROOT_CREDENTIAL" -H "Polaris-Realm: $REALM" \
  -d grant_type=client_credentials -d scope=PRINCIPAL_ROLE:ALL | jq -r .access_token)
[[ -n $TOKEN && $TOKEN != null ]] || { echo "no token" >&2; exit 1; }

storage=$(jq -n \
  --arg loc "$BASE_LOCATION" --arg region "$AWS_REGION" \
  --arg ep "$S3_ENDPOINT" --arg epi "$S3_ENDPOINT_INTERNAL" \
  --arg role "$STORAGE_ROLE_ARN" --arg ext "$STORAGE_EXTERNAL_ID" '
  {storageType: "S3", allowedLocations: [$loc], region: $region}
  + (if $ep  != "" then {endpoint: $ep, pathStyleAccess: true} else {} end)
  + (if $epi != "" then {endpointInternal: $epi} else {} end)
  + (if $role != "" then {roleArn: $role} else {} end)
  + (if $ext != "" then {externalId: $ext} else {} end)')

echo "Creating catalog $CATALOG at $BASE_LOCATION ..."
api POST /catalogs "$(jq -n --arg n "$CATALOG" --arg loc "$BASE_LOCATION" --argjson s "$storage" '
  {catalog: {name: $n, type: "INTERNAL", readOnly: false,
             properties: {"default-base-location": $loc}, storageConfigInfo: $s}}')" >/dev/null

echo "Granting content management to catalog_admin (root's role) ..."
api PUT "/catalogs/$CATALOG/catalog-roles/catalog_admin/grants" \
  '{"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}}' >/dev/null

echo "Creating read-only path for Snowflake ..."
api POST /principal-roles '{"principalRole": {"name": "snowflake_reader_role"}}' >/dev/null
api POST "/catalogs/$CATALOG/catalog-roles" '{"catalogRole": {"name": "reader"}}' >/dev/null
for p in TABLE_READ_DATA TABLE_LIST NAMESPACE_LIST TABLE_FULL_METADATA NAMESPACE_FULL_METADATA CATALOG_READ_PROPERTIES; do
  api PUT "/catalogs/$CATALOG/catalog-roles/reader/grants" \
    "{\"grant\": {\"type\": \"catalog\", \"privilege\": \"$p\"}}" >/dev/null
done
api PUT "/principal-roles/snowflake_reader_role/catalog-roles/$CATALOG" \
  '{"catalogRole": {"name": "reader"}}' >/dev/null

created=$(api POST /principals '{"principal": {"name": "snowflake_reader"}, "credentialRotationRequired": false}')
api PUT /principals/snowflake_reader/principal-roles \
  '{"principalRole": {"name": "snowflake_reader_role"}}' >/dev/null

if [[ -n $created ]]; then
  id=$(jq -r '.credentials.clientId' <<<"$created")
  secret=$(jq -r '.credentials.clientSecret' <<<"$created")
  umask 077
  printf 'POLARIS_READER_CLIENT_ID=%s\nPOLARIS_READER_CLIENT_SECRET=%s\n' "$id" "$secret" > "$READER_ENV_FILE"
  echo "Reader credentials written to $READER_ENV_FILE (shown once by Polaris; keep it out of git)."
else
  echo "snowflake_reader already existed; its secret is not shown again. Rotate it if you lost it."
fi

echo "Done. Catalog: $CATALOG"
