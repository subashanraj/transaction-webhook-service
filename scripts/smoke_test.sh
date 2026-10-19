#!/usr/bin/env bash
# Walks through the assessment's success criteria against a running service.
#   ./scripts/smoke_test.sh https://your-service.onrender.com
#   QUICK=1 ./scripts/smoke_test.sh http://localhost:8000   # skip the 30 s wait
set -u
BASE="${1:-http://localhost:8000}"
BASE="${BASE%/}"
RUN="$(date +%s)$RANDOM"
PASS=0; FAIL=0

ok()   { echo "  PASS  $1"; PASS=$((PASS+1)); }
bad()  { echo "  FAIL  $1"; FAIL=$((FAIL+1)); }
check() { if [ "$2" = "$3" ]; then ok "$1"; else bad "$1 (expected '$3', got '$2')"; fi; }
lt()   { awk -v a="$2" -v b="$3" 'BEGIN{exit !(a<b)}' && ok "$1 (${2}s)" || bad "$1 (${2}s >= ${3}s)"; }

body() {
  printf '{"transaction_id":"%s","source_account":"acc_user_789","destination_account":"acc_merchant_456","amount":%s,"currency":"%s"}' "$1" "${2:-1500}" "${3:-INR}"
}
post() { curl -s -o /dev/null -w "%{http_code} %{time_total}" -X POST "$BASE/v1/webhooks/transactions" -H "Content-Type: application/json" -d "$1"; }
field() { python -c "import sys,json; d=json.load(sys.stdin); d=d[0] if isinstance(d,list) else d; print(d.get('$1'))"; }

echo "Target: $BASE"

echo "[S0] health check"
check "GET / -> HEALTHY" "$(curl -s "$BASE/" | field status)" "HEALTHY"

echo "[S1] single transaction"
TID="txn_smoke_${RUN}"
read -r code t <<<"$(post "$(body "$TID")")"
check "POST -> 202" "$code" "202"
lt "POST responds < 500ms" "$t" 0.5
check "status right after POST" "$(curl -s "$BASE/v1/transactions/$TID" | field status)" "PROCESSING"

echo "[S2] same webhook 5x"
for i in 1 2 3 4 5; do read -r code t <<<"$(post "$(body "$TID")")"; check "duplicate #$i -> 202" "$code" "202"; done
check "query returns one record" "$(curl -s "$BASE/v1/transactions/$TID" | python -c 'import sys,json; print(len(json.load(sys.stdin)))')" "1"

echo "[S3] 20 concurrent duplicates"
TID3="txn_smoke_race_${RUN}"
TMP="$(mktemp -d)"
for i in $(seq 1 20); do post "$(body "$TID3" 42)" > "$TMP/race_$i" & done; wait
check "all 202" "$(for f in "$TMP"/race_*; do cat "$f"; echo; done | awk '$1==202' | wc -l | tr -d ' ')" "20"

echo "[S6] 50 distinct webhooks in parallel"
for i in $(seq 1 50); do post "$(body "txn_smoke_burst_${RUN}_$i" "$i")" > "$TMP/burst_$i" & done; wait
out="$(for f in "$TMP"/burst_*; do cat "$f"; echo; done)"
check "all 202" "$(echo "$out" | awk '$1==202' | wc -l | tr -d ' ')" "50"
slowest=$(echo "$out" | awk 'NF{print $2}' | sort -n | tail -1)
rm -rf "$TMP"
lt "slowest response < 500ms" "$slowest" 0.5

echo "[S20/S8] negative cases"
check "unknown id -> 404" "$(curl -s -o /dev/null -w '%{http_code}' "$BASE/v1/transactions/txn_nope_$RUN")" "404"
read -r code t <<<"$(post '{"transaction_id":"x","amount":-1}')"
check "invalid body -> 422" "$code" "422"

if [ -z "${QUICK:-}" ]; then
  echo "[S1] waiting 35s for processing..."
  sleep 35
  R=$(curl -s "$BASE/v1/transactions/$TID")
  check "status after ~30s" "$(echo "$R" | field status)" "PROCESSED"
  echo "      created_at=$(echo "$R" | field created_at) processed_at=$(echo "$R" | field processed_at)"
  check "race txn processed" "$(curl -s "$BASE/v1/transactions/$TID3" | field status)" "PROCESSED"
fi

echo
echo "Result: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
