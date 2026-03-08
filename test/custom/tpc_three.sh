set -e

BASE_URL="${BASE_URL:-http://localhost:8000}"
COMPOSE_PROJECT="${COMPOSE_PROJECT:-wdm-project-group24}"  # adjust if your project name differs
ORDER_DB_CONTAINER="${ORDER_DB_CONTAINER:-wdm-project-group24-order-db-1}"   # or order-db if you use that name
STOCK_DB_CONTAINER="${STOCK_DB_CONTAINER:-wdm-project-group24-stock-db-1}"
ORDER_SERVICE_CONTAINER="${ORDER_SERVICE_CONTAINER:-wdm-project-group24-order-service-1}"
STOCK_SERVICE_CONTAINER="${STOCK_SERVICE_CONTAINER:-wdm-project-group24-stock-service-1}"

PASS=0
FAIL=0

echo "Phase 5 — Fault tolerance (recovery) tests"
echo "Base URL: $BASE_URL"
echo ""

json_field() {
  python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('$1',''))" 2>/dev/null || echo ""
}

expect_status() {
  local desc="$1"
  local expected="$2"
  shift 2
  local code
  code=$(curl -s -o /dev/null -w "%{http_code}" "$@")
  if [ "$code" = "$expected" ]; then
    echo "  [PASS] $desc (HTTP $code)"
    ((PASS++)) || true
    return 0
  else
    echo "  [FAIL] $desc (expected HTTP $expected, got $code)"
    ((FAIL++)) || true
    return 1
  fi
}

# --- 5a: Coordinator recovery ---
echo "=== 5a: Coordinator recovery (Order) ==="
echo "  Setup: create user, item, order; run 2PC checkout so we have a committed txn and order_id."

USER_ID=$(curl -s -X POST "$BASE_URL/payment/create_user" | json_field "user_id")
curl -s -o /dev/null -X POST "$BASE_URL/payment/add_funds/$USER_ID/200"
ITEM_ID=$(curl -s -X POST "$BASE_URL/stock/item/create/10" | json_field "item_id")
curl -s -o /dev/null -X POST "$BASE_URL/stock/add/$ITEM_ID/20"
ORDER_ID=$(curl -s -X POST "$BASE_URL/orders/create/$USER_ID" | json_field "order_id")
curl -s -o /dev/null -X POST "$BASE_URL/orders/addItem/$ORDER_ID/$ITEM_ID/3"
curl -s -o /dev/null -X POST "$BASE_URL/orders/checkout/$ORDER_ID"
echo "  Order $ORDER_ID checked out (committed)."

echo "  Inject incomplete txn: insert transaction_log row (status=committing) and set order unpaid."
RECOVERY_TXN_ID="recovery-txn-$(date +%s)"
# prepared_stock: one item, quantity 3. prepared_payment: true.
PREPARED_STOCK='[["'"$ITEM_ID"'", 3]]'
docker exec "$ORDER_DB_CONTAINER" psql -U user -d orders -v on_error_stop=1 -c "
  INSERT INTO transaction_log (txn_id, order_id, status, prepared_stock, prepared_payment, user_id, total_cost, created_at)
  VALUES ('$RECOVERY_TXN_ID', '$ORDER_ID', 'committing', '$PREPARED_STOCK', true, '$USER_ID', 30, NOW());
  UPDATE orders SET paid = false WHERE id = '$ORDER_ID';
" > /dev/null 2>&1
echo "  Incomplete txn $RECOVERY_TXN_ID inserted; order $ORDER_ID set unpaid."

echo "  Restart Order service to trigger coordinator recovery."
docker restart "$ORDER_SERVICE_CONTAINER" > /dev/null 2>&1
echo "  Waiting for Order service to be ready..."
sleep 5
for i in 1 2 3 4 5 6 7 8 9 10; do
  if curl -s -o /dev/null -w "%{http_code}" "$BASE_URL/orders/create/$USER_ID" | grep -q 200; then
    break
  fi
  sleep 2
done

echo "  Verify: order $ORDER_ID is paid (recovery should have completed commit)."
PAID=$(curl -s "$BASE_URL/orders/find/$ORDER_ID" | json_field "paid")
if [ "$PAID" = "True" ] || [ "$PAID" = "true" ]; then
  echo "  [PASS] Order is paid after recovery."
  ((PASS++)) || true
else
  echo "  [FAIL] Order still unpaid after recovery (paid=$PAID)."
  ((FAIL++)) || true
fi
echo ""

# --- 5b: Participant recovery (Stock) ---
echo "=== 5b: Participant recovery (Stock) ==="
echo "  Setup: new item, add stock; prepare 1 unit (stock decreases)."

ITEM2=$(curl -s -X POST "$BASE_URL/stock/item/create/5" | json_field "item_id")
curl -s -o /dev/null -X POST "$BASE_URL/stock/add/$ITEM2/10"
STOCK_BEFORE=$(curl -s "$BASE_URL/stock/find/$ITEM2" | json_field "stock")
curl -s -o /dev/null -X POST "$BASE_URL/stock/prepare/txn-stale/$ITEM2/1"
STOCK_AFTER_PREPARE=$(curl -s "$BASE_URL/stock/find/$ITEM2" | json_field "stock")
echo "  Item $ITEM2 stock: $STOCK_BEFORE -> $STOCK_AFTER_PREPARE after prepare 1."

echo "  Inject stale prepared_transactions row (created_at = 6 minutes ago)."
docker exec "$STOCK_DB_CONTAINER" psql -U user -d stock -v on_error_stop=1 -c "
  INSERT INTO prepared_transactions (txn_id, item_id, quantity, created_at)
  VALUES ('txn-stale-old', '$ITEM2', 1, NOW() - INTERVAL '6 minutes')
  ON CONFLICT (txn_id, item_id) DO NOTHING;
" > /dev/null 2>&1
echo "  Stale row inserted for txn-stale-old, item $ITEM2, quantity 1."

echo "  Restart Stock service to trigger participant recovery."
docker restart "$STOCK_SERVICE_CONTAINER" > /dev/null 2>&1
echo "  Waiting for Stock service to be ready..."
sleep 5
for i in 1 2 3 4 5 6 7 8 9 10; do
  if curl -s -o /dev/null -w "%{http_code}" "$BASE_URL/stock/find/$ITEM2" | grep -q 200; then
    break
  fi
  sleep 2
done

echo "  Verify: stock for $ITEM2 increased by 1 (stale prepared txn aborted = restore 1)."
STOCK_AFTER_RECOVERY=$(curl -s "$BASE_URL/stock/find/$ITEM2" | json_field "stock")
EXPECTED=$((STOCK_AFTER_PREPARE + 1))
if [ "$STOCK_AFTER_RECOVERY" = "$EXPECTED" ]; then
  echo "  [PASS] Stock after recovery = $STOCK_AFTER_RECOVERY (expected $EXPECTED)."
  ((PASS++)) || true
else
  echo "  [FAIL] Stock after recovery = $STOCK_AFTER_RECOVERY (expected $EXPECTED)."
  ((FAIL++)) || true
fi
echo ""

# --- Summary ---
echo "=== Summary ==="
echo "  Passed: $PASS  Failed: $FAIL"
if [ "$FAIL" -eq 0 ]; then
  echo "  Result: Phase 5 recovery tests passed."
  exit 0
else
  echo "  Result: Some Phase 5 recovery tests failed."
  exit 1
fi