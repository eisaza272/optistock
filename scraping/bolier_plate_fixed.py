import os
import requests

url = "https://api.alegra.com/api/v1/purchase-orders?order_direction=ASC"

AUTHORIZATION_TOKEN = os.getenv("KEY_ALEGRA")
if not AUTHORIZATION_TOKEN:
    raise ValueError("KEY_ALEGRA environment variable is not set")

headers = {
    "accept": "application/json",
    "authorization": AUTHORIZATION_TOKEN
}

response = requests.get(url, headers=headers)
items = response.json()

for item in items:
    dict_ = { 
        "invoice_id": item["id"],
        "added_inventory_date": item["deliveryDate"],
        "provider_id": item["id"],
        "warehouse_name": item["warehouse"]["name"],
    }

    for item_ in item["purchases"]["items"]:
        dict_["price_provider"] = item_["price"]
        dict_["quantity"] = item_["quantity"]
        dict_["item_id"] = item_["id"]
        dict_["item_name"] = item_["name"]
