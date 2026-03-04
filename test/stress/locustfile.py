from locust import HttpUser, task
from json import JSONDecodeError
from locust import events
import requests
import random
from dataclasses import dataclass

@dataclass
class Product:
    id: str
    name: str
    price: int

class HelloWorldUser(HttpUser):
    @task
    def user_full_checkout(self):
        order_id = self.client.post(f"http://localhost:8000/orders/create/{self.uid}").json()["order_id"]

        n_products = random.randint(1, 10)

        for _ in range(n_products):
            prod = random.choice(products)
            quantity = random.randint(1,20)
            self.client.post(f"http://localhost:8000/orders/addItem/{order_id}/{prod.id}/{quantity}")

        self.client.post(f"http://localhost:8000/orders/checkout/{order_id}")

    def on_start(self):
        # Create the user
        self.uid = self.client.post("http://localhost:8000/payment/create_user", catch_response=True).json()["user_id"]

        # Add funds
        amount = random.randint(50, 500)
        self.client.post(f"http://localhost:8000/payment/add_funds/{self.uid}/{amount}")

    @events.test_start.add_listener
    def on_test_start(environment, **kwargs):
        # Create products
        global products
        products = [
            Product(id="", name="banana", price=3),
            Product(id="", name="pencil", price=5),
            Product(id="", name="notebook", price=8),
            Product(id="", name="apple", price=4),
            Product(id="", name="eraser", price=2),
            Product(id="", name="water bottle", price=15),
        ]

        for p in products:
            # TODO find another way without harcoding the url here
            p.id = requests.post(f"http://localhost:8000/stock/item/create/{p.price}").json()["item_id"]

            # Add random amount to the stock
            amount = random.randint(200, 1000)
            requests.post(f"http://localhost:8000/stock/add/{p.id}/{amount}")

        print (products)
