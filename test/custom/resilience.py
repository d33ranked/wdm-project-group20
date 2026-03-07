# Terminal 1: System running (docker-compose up)

# Terminal 2: Setup
python3 -c "
import requests, json
G = 'http://localhost:8000'
user = requests.post(f'{G}/payment/create_user').json()['user_id']
requests.post(f'{G}/payment/add_funds/{user}/100')
item = requests.post(f'{G}/stock/item/create/10').json()['item_id']
requests.post(f'{G}/stock/add/{item}/5')
order = requests.post(f'{G}/orders/create/{user}').json()['order_id']
requests.post(f'{G}/orders/addItem/{order}/{item}/2')
print(f'ORDER_ID={order}')
"

# Terminal 3: Kill stock for 1 second, then bring it back
docker-compose stop stock-service && sleep 1 && docker-compose start stock-service

# Terminal 2 (immediately after running the kill command above):
# Trigger checkout during the outage window:
curl -s -X POST "http://localhost:8000/orders/checkout/$ORDER_ID"
# With retries: should succeed (stock service comes back within retry window)
# Without retries: would fail immediately