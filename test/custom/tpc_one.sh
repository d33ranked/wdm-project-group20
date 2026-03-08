#!/usr/bin/env bash
# Phase 1 2PC tests: Stock and Payment prepare/commit/abort.
# Run with gateway and services up (e.g. docker-compose up).
# Usage: ./test/test_2pc_phase1.sh   or   bash test/test_2pc_phase1.sh

set -e

BASE_URL="${BASE_URL:-http://localhost:8000}"
STOCK_PASS=0
STOCK_FAIL=0
PAYMENT_PASS=0
PAYMENT_FAIL=0

echo "Base URL: $BASE_URL"
echo ""

# --- Helpers ---
# Expect status code. Usage: expect_status "description" "curl args..." expected_code
expect_status() {
  local desc="$1"
  local expected="$2"
  shift 2
  local code
  code=$(curl -s -o /dev/null -w "%{http_code}" "$@")
  if [ "$code" = "$expected" ]; then
    echo "  [PASS] $desc (HTTP $code)"
    return 0
  else
    echo "  [FAIL] $desc (expected HTTP $expected, got $code)"
    return 1
  fi
}

# Get JSON field. Usage: json_field "key"
json_field() {
  python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('$1',''))" 2>/dev/null || echo ""
}

# --- Stock setup ---
echo "=== Stock service 2PC tests ==="
ITEM=$(curl -s -X POST "$BASE_URL/stock/item/create/10" | json_field "item_id")
curl -s -o /dev/null -X POST "$BASE_URL/stock/add/$ITEM/10"
STOCK_BEFORE=$(curl -s "$BASE_URL/stock/find/$ITEM" | json_field "stock")
echo "  Setup: item=$ITEM, stock=$STOCK_BEFORE"

# Stock 1: prepare then commit
if expect_status "prepare (reserve 3)" "200" -X POST "$BASE_URL/stock/prepare/txn-s1/$ITEM/3"; then ((STOCK_PASS++)); else ((STOCK_FAIL++)); fi
STOCK_MID=$(curl -s "$BASE_URL/stock/find/$ITEM" | json_field "stock")
if [ "$STOCK_MID" = "7" ]; then echo "  [PASS] stock after prepare = 7"; ((STOCK_PASS++)); else echo "  [FAIL] stock after prepare (expected 7, got $STOCK_MID)"; ((STOCK_FAIL++)); fi
if expect_status "commit txn-s1" "200" -X POST "$BASE_URL/stock/commit/txn-s1"; then ((STOCK_PASS++)); else ((STOCK_FAIL++)); fi
STOCK_AFTER=$(curl -s "$BASE_URL/stock/find/$ITEM" | json_field "stock")
if [ "$STOCK_AFTER" = "7" ]; then echo "  [PASS] stock after commit still 7"; ((STOCK_PASS++)); else echo "  [FAIL] stock after commit (expected 7, got $STOCK_AFTER)"; ((STOCK_FAIL++)); fi

# Stock 2: prepare then abort
if expect_status "prepare txn-s2 (reserve 3)" "200" -X POST "$BASE_URL/stock/prepare/txn-s2/$ITEM/3"; then ((STOCK_PASS++)); else ((STOCK_FAIL++)); fi
if expect_status "abort txn-s2" "200" -X POST "$BASE_URL/stock/abort/txn-s2"; then ((STOCK_PASS++)); else ((STOCK_FAIL++)); fi
STOCK_ABORT=$(curl -s "$BASE_URL/stock/find/$ITEM" | json_field "stock")
if [ "$STOCK_ABORT" = "7" ]; then echo "  [PASS] stock after abort restored to 7"; ((STOCK_PASS++)); else echo "  [FAIL] stock after abort (expected 7, got $STOCK_ABORT)"; ((STOCK_FAIL++)); fi

# Stock 3: insufficient stock (VOTE NO)
if expect_status "prepare 999 units (expect 400)" "400" -X POST "$BASE_URL/stock/prepare/txn-s3/$ITEM/999"; then ((STOCK_PASS++)); else ((STOCK_FAIL++)); fi
STOCK_NO=$(curl -s "$BASE_URL/stock/find/$ITEM" | json_field "stock")
if [ "$STOCK_NO" = "7" ]; then echo "  [PASS] stock unchanged after vote NO"; ((STOCK_PASS++)); else echo "  [FAIL] stock after vote NO (expected 7, got $STOCK_NO)"; ((STOCK_FAIL++)); fi

# Stock 4: commit idempotency
if expect_status "commit txn-s1 again (idempotent)" "200" -X POST "$BASE_URL/stock/commit/txn-s1"; then ((STOCK_PASS++)); else ((STOCK_FAIL++)); fi

# Stock 5: abort idempotency (no such txn)
if expect_status "abort txn-s99 (idempotent)" "200" -X POST "$BASE_URL/stock/abort/txn-s99"; then ((STOCK_PASS++)); else ((STOCK_FAIL++)); fi

echo "  Stock: $STOCK_PASS passed, $STOCK_FAIL failed"
echo ""

# --- Payment setup ---
echo "=== Payment service 2PC tests ==="
USER_JSON=$(curl -s -X POST "$BASE_URL/payment/create_user")
USER_ID=$(echo "$USER_JSON" | json_field "user_id")
curl -s -o /dev/null -X POST "$BASE_URL/payment/add_funds/$USER_ID/100"
CREDIT_BEFORE=$(curl -s "$BASE_URL/payment/find_user/$USER_ID" | json_field "credit")
echo "  Setup: user=$USER_ID, credit=$CREDIT_BEFORE"

# Payment 1: prepare then commit
if expect_status "prepare (reserve 30)" "200" -X POST "$BASE_URL/payment/prepare/txn-p1/$USER_ID/30"; then ((PAYMENT_PASS++)); else ((PAYMENT_FAIL++)); fi
CREDIT_MID=$(curl -s "$BASE_URL/payment/find_user/$USER_ID" | json_field "credit")
if [ "$CREDIT_MID" = "70" ]; then echo "  [PASS] credit after prepare = 70"; ((PAYMENT_PASS++)); else echo "  [FAIL] credit after prepare (expected 70, got $CREDIT_MID)"; ((PAYMENT_FAIL++)); fi
if expect_status "commit txn-p1" "200" -X POST "$BASE_URL/payment/commit/txn-p1"; then ((PAYMENT_PASS++)); else ((PAYMENT_FAIL++)); fi
CREDIT_AFTER=$(curl -s "$BASE_URL/payment/find_user/$USER_ID" | json_field "credit")
if [ "$CREDIT_AFTER" = "70" ]; then echo "  [PASS] credit after commit still 70"; ((PAYMENT_PASS++)); else echo "  [FAIL] credit after commit (expected 70, got $CREDIT_AFTER)"; ((PAYMENT_FAIL++)); fi

# Payment 2: prepare then abort
if expect_status "prepare txn-p2 (reserve 20)" "200" -X POST "$BASE_URL/payment/prepare/txn-p2/$USER_ID/20"; then ((PAYMENT_PASS++)); else ((PAYMENT_FAIL++)); fi
if expect_status "abort txn-p2" "200" -X POST "$BASE_URL/payment/abort/txn-p2"; then ((PAYMENT_PASS++)); else ((PAYMENT_FAIL++)); fi
CREDIT_ABORT=$(curl -s "$BASE_URL/payment/find_user/$USER_ID" | json_field "credit")
if [ "$CREDIT_ABORT" = "70" ]; then echo "  [PASS] credit after abort restored to 70"; ((PAYMENT_PASS++)); else echo "  [FAIL] credit after abort (expected 70, got $CREDIT_ABORT)"; ((PAYMENT_FAIL++)); fi

# Payment 3: insufficient credit (VOTE NO)
if expect_status "prepare 999 (expect 400)" "400" -X POST "$BASE_URL/payment/prepare/txn-p3/$USER_ID/999"; then ((PAYMENT_PASS++)); else ((PAYMENT_FAIL++)); fi
CREDIT_NO=$(curl -s "$BASE_URL/payment/find_user/$USER_ID" | json_field "credit")
if [ "$CREDIT_NO" = "70" ]; then echo "  [PASS] credit unchanged after vote NO"; ((PAYMENT_PASS++)); else echo "  [FAIL] credit after vote NO (expected 70, got $CREDIT_NO)"; ((PAYMENT_FAIL++)); fi

# Payment 4: commit idempotency
if expect_status "commit txn-p1 again (idempotent)" "200" -X POST "$BASE_URL/payment/commit/txn-p1"; then ((PAYMENT_PASS++)); else ((PAYMENT_FAIL++)); fi

# Payment 5: abort idempotency (no such txn)
if expect_status "abort txn-p99 (idempotent)" "200" -X POST "$BASE_URL/payment/abort/txn-p99"; then ((PAYMENT_PASS++)); else ((PAYMENT_FAIL++)); fi

echo "  Payment: $PAYMENT_PASS passed, $PAYMENT_FAIL failed"
echo ""

# --- Summary ---
TOTAL_PASS=$((STOCK_PASS + PAYMENT_PASS))
TOTAL_FAIL=$((STOCK_FAIL + PAYMENT_FAIL))
echo "=== Summary ==="
echo "  Stock:   $STOCK_PASS passed, $STOCK_FAIL failed"
echo "  Payment: $PAYMENT_PASS passed, $PAYMENT_FAIL failed"
echo "  Total:   $TOTAL_PASS passed, $TOTAL_FAIL failed"

if [ "$TOTAL_FAIL" -eq 0 ]; then
  echo "  Result:  All Phase 1 2PC tests passed."
  exit 0
else
  echo "  Result:  Some tests failed."
  exit 1
fi
