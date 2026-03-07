import requests

GATEWAY = "http://localhost:8000"


def test_normal_checkout():
    """Verify retries don't break normal operation."""
    print("Test A: Normal checkout with retry logic...")
    user = requests.post(f"{GATEWAY}/payment/create_user").json()["user_id"]
    requests.post(f"{GATEWAY}/payment/add_funds/{user}/100")
    item = requests.post(f"{GATEWAY}/stock/item/create/10").json()["item_id"]
    requests.post(f"{GATEWAY}/stock/add/{item}/5")

    order = requests.post(f"{GATEWAY}/orders/create/{user}").json()["order_id"]
    requests.post(f"{GATEWAY}/orders/addItem/{order}/{item}/2")

    r = requests.post(f"{GATEWAY}/orders/checkout/{order}")
    assert r.status_code == 200, f"Checkout failed: {r.text}"

    stock = requests.get(f"{GATEWAY}/stock/find/{item}").json()["stock"]
    credit = requests.get(f"{GATEWAY}/payment/find_user/{user}").json()["credit"]
    assert stock == 3, f"Expected stock=3, got {stock}"
    assert credit == 80, f"Expected credit=80, got {credit}"
    print("  PASSED")


def test_failure_still_fails():
    """Verify that business failures (4xx) are NOT retried."""
    print("Test B: Business failures not retried...")
    user = requests.post(f"{GATEWAY}/payment/create_user").json()["user_id"]
    requests.post(f"{GATEWAY}/payment/add_funds/{user}/10")  # only 10 credit
    item = requests.post(f"{GATEWAY}/stock/item/create/999").json()[
        "item_id"
    ]  # price 999
    requests.post(f"{GATEWAY}/stock/add/{item}/5")

    order = requests.post(f"{GATEWAY}/orders/create/{user}").json()["order_id"]
    requests.post(f"{GATEWAY}/orders/addItem/{order}/{item}/1")

    r = requests.post(f"{GATEWAY}/orders/checkout/{order}")
    assert r.status_code == 400, "Should have failed due to insufficient credit"

    stock = requests.get(f"{GATEWAY}/stock/find/{item}").json()["stock"]
    assert stock == 5, f"Stock should be unchanged at 5, got {stock}"
    print("  PASSED — stock rolled back, failure was not retried")


if __name__ == "__main__":
    test_normal_checkout()
    test_failure_still_fails()
    print("\nALL RETRY TESTS PASSED")
