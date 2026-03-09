import uuid
import requests
import concurrent.futures

GATEWAY = "http://localhost:8000"

def test_1_basic_idempotency():
    """Same key twice → stock only subtracted once."""
    print("Test 1: Basic idempotency...")
    item = requests.post(f"{GATEWAY}/stock/item/create/10").json()["item_id"]
    requests.post(f"{GATEWAY}/stock/add/{item}/10")

    key = f"test-{uuid.uuid4()}"
    headers = {"Idempotency-Key": key}

    r1 = requests.post(f"{GATEWAY}/stock/subtract/{item}/3", headers=headers)
    r2 = requests.post(f"{GATEWAY}/stock/subtract/{item}/3", headers=headers)

    assert r1.status_code == 200, f"First call failed: {r1.status_code}"
    assert r2.status_code == 200, f"Second call failed: {r2.status_code}"

    stock = requests.get(f"{GATEWAY}/stock/find/{item}").json()["stock"]
    assert stock == 7, f"Expected 7, got {stock} — double subtraction!"
    print("  PASSED — stock subtracted only once")


def test_2_no_key_works_normally():
    """Without idempotency key, every call executes."""
    print("Test 2: No key = normal behavior...")
    item = requests.post(f"{GATEWAY}/stock/item/create/10").json()["item_id"]
    requests.post(f"{GATEWAY}/stock/add/{item}/10")

    requests.post(f"{GATEWAY}/stock/subtract/{item}/3")  # no header
    requests.post(f"{GATEWAY}/stock/subtract/{item}/3")  # no header

    stock = requests.get(f"{GATEWAY}/stock/find/{item}").json()["stock"]
    assert stock == 4, f"Expected 4, got {stock}"
    print("  PASSED — both calls executed without key")


def test_3_different_keys_both_execute():
    """Different keys → both execute (they're different operations)."""
    print("Test 3: Different keys = both execute...")
    item = requests.post(f"{GATEWAY}/stock/item/create/10").json()["item_id"]
    requests.post(f"{GATEWAY}/stock/add/{item}/10")

    r1 = requests.post(f"{GATEWAY}/stock/subtract/{item}/2", headers={"Idempotency-Key": f"key-{uuid.uuid4()}"})
    r2 = requests.post(f"{GATEWAY}/stock/subtract/{item}/2", headers={"Idempotency-Key": f"key-{uuid.uuid4()}"})

    assert r1.status_code == 200 and r2.status_code == 200
    stock = requests.get(f"{GATEWAY}/stock/find/{item}").json()["stock"]
    assert stock == 6, f"Expected 6, got {stock}"
    print("  PASSED — different keys both executed")


def test_4_concurrent_same_key():
    """Two concurrent requests with same key → only one executes."""
    print("Test 4: Concurrent same key...")
    item = requests.post(f"{GATEWAY}/stock/item/create/10").json()["item_id"]
    requests.post(f"{GATEWAY}/stock/add/{item}/10")

    key = f"concurrent-{uuid.uuid4()}"

    def call(_):
        return requests.post(
            f"{GATEWAY}/stock/subtract/{item}/3",
            headers={"Idempotency-Key": key}
        ).status_code

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
        results = list(pool.map(call, range(5)))

    stock = requests.get(f"{GATEWAY}/stock/find/{item}").json()["stock"]
    assert stock == 7, f"Expected 7, got {stock} — concurrent idempotency failed!"
    print(f"  PASSED — stock=7, results: {results}")


def test_5_payment_idempotency():
    """Idempotency works for payment too."""
    print("Test 5: Payment idempotency...")
    user = requests.post(f"{GATEWAY}/payment/create_user").json()["user_id"]
    requests.post(f"{GATEWAY}/payment/add_funds/{user}/100")

    key = f"pay-{uuid.uuid4()}"
    headers = {"Idempotency-Key": key}

    r1 = requests.post(f"{GATEWAY}/payment/pay/{user}/30", headers=headers)
    r2 = requests.post(f"{GATEWAY}/payment/pay/{user}/30", headers=headers)

    credit = requests.get(f"{GATEWAY}/payment/find_user/{user}").json()["credit"]
    assert credit == 70, f"Expected 70, got {credit}"
    print("  PASSED — payment deducted only once")


def test_6_checkout_idempotency():
    """Full checkout with idempotency keys doesn't double-charge."""
    print("Test 6: Checkout uses idempotency keys...")
    user = requests.post(f"{GATEWAY}/payment/create_user").json()["user_id"]
    requests.post(f"{GATEWAY}/payment/add_funds/{user}/200")
    item = requests.post(f"{GATEWAY}/stock/item/create/10").json()["item_id"]
    requests.post(f"{GATEWAY}/stock/add/{item}/10")

    order = requests.post(f"{GATEWAY}/orders/create/{user}").json()["order_id"]
    requests.post(f"{GATEWAY}/orders/addItem/{order}/{item}/2")

    r = requests.post(f"{GATEWAY}/orders/checkout/{order}")
    assert r.status_code == 200

    stock = requests.get(f"{GATEWAY}/stock/find/{item}").json()["stock"]
    credit = requests.get(f"{GATEWAY}/payment/find_user/{user}").json()["credit"]
    assert stock == 8, f"Expected stock=8, got {stock}"
    assert credit == 180, f"Expected credit=180, got {credit}"
    print("  PASSED — checkout worked correctly with idempotency keys")


if __name__ == "__main__":
    test_1_basic_idempotency()
    test_2_no_key_works_normally()
    test_3_different_keys_both_execute()
    test_4_concurrent_same_key()
    test_5_payment_idempotency()
    test_6_checkout_idempotency()
    print("\nALL IDEMPOTENCY TESTS PASSED")