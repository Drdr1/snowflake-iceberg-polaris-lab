#!/usr/bin/env bash
# Prove the Snowflake principal is read-only: it can list, it cannot create.
set -euo pipefail

POLARIS_URL="${POLARIS_URL:-http://localhost:8181}"
REALM="${POLARIS_REALM:-POLARIS}"
CATALOG="${POLARIS_CATALOG:-lab_catalog}"
READER_ENV_FILE="${READER_ENV_FILE:-.polaris-reader.env}"

# shellcheck disable=SC1090
source "$READER_ENV_FILE"

TOKEN=$(curl -sS --fail-with-body "$POLARIS_URL/api/catalog/v1/oauth/tokens" \
  --user "$POLARIS_READER_CLIENT_ID:$POLARIS_READER_CLIENT_SECRET" -H "Polaris-Realm: $REALM" \
  -d grant_type=client_credentials -d scope=PRINCIPAL_ROLE:ALL | jq -r .access_token)

status() {  # status METHOD PATH [JSON]
  curl -sS -o /dev/null -w '%{http_code}' -X "$1" \
    -H "Authorization: Bearer $TOKEN" -H "Polaris-Realm: $REALM" -H 'Content-Type: application/json' \
    ${3:+-d "$3"} "$POLARIS_URL/api/catalog/v1/$CATALOG$2"
}

fail=0
expect() {  # expect WANT GOT LABEL
  if [[ $1 == "$2" ]]; then echo "ok   $3 -> $2"; else echo "FAIL $3 -> $2 (wanted $1)"; fail=1; fi
}

expect 200 "$(status GET /namespaces)" "reader lists namespaces"
expect 403 "$(status POST /namespaces '{"namespace":["reader_should_not_create"]}')" "reader creates namespace"
expect 403 "$(status DELETE /namespaces/lab)" "reader drops namespace"
# lab.orders exists: CI runs spark.load_demo before this check.
expect 200 "$(status GET /namespaces/lab/tables/orders)" "reader loads table metadata"
expect 403 "$(status POST /namespaces/lab/tables '{"name":"reader_table","schema":{"type":"struct","fields":[{"id":1,"name":"x","type":"int","required":false}]}}')" "reader creates table"
expect 403 "$(status DELETE /namespaces/lab/tables/orders)" "reader drops table"

exit $fail
