import os
import requests
import json

url = "https://api.alegra.com/api/v1/items"

AUTHORIZATION_TOKEN = os.getenv("KEY_ALEGRA")
if not AUTHORIZATION_TOKEN:
    raise ValueError("KEY_ALEGRA environment variable is not set")

headers = {
    "accept": "application/json",
    "authorization": AUTHORIZATION_TOKEN
}

response = requests.get(url, headers=headers)

items = response.json()

# Save items to JSON file
with open('items.json', 'w', encoding='utf-8') as f:
    json.dump(items, f, indent=2, ensure_ascii=False)

print(f"Items saved to items.json")

for item in items:
