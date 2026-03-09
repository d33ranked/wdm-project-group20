import requests
import concurrent.futures

GATEWAY = "http://localhost:8000"

# Create item with exactly 5 stock
item_resp = requests.post(f"{GATEWAY}/stock/item/create/10")
item_id = item_resp.json()["item_id"]
requests.post(f"{GATEWAY}/stock/add/{item_id}/5")

print(f"Created item {item_id} with stock=5, price=10")


# Fire 10 concurrent subtract-1 requests
def subtract_one(i):
    r = requests.post(f"{GATEWAY}/stock/subtract/{item_id}/1")
    return r.status_code


with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
    results = list(pool.map(subtract_one, range(10)))

successes = results.count(200)
failures = [r for r in results if r != 200]
final_stock = requests.get(f"{GATEWAY}/stock/find/{item_id}").json()["stock"]

print(f"Results: {successes} successes, {len(failures)} failures")
print(f"Final stock: {final_stock}")

assert successes == 5, f"Expected exactly 5 successes, got {successes}"
assert final_stock == 0, f"Expected stock=0, got {final_stock}"
print("\nRACE CONDITION TEST PASSED — PostgreSQL FOR UPDATE works!")
