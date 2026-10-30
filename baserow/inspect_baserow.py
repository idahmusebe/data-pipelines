import os
import requests

API_TOKEN = os.getenv("BASEROW_API_TOKEN")

TABLES = {
    "customers": 1291093,
    "products": 1291171,
    "orders": 1291067,
}

if not API_TOKEN:
    raise ValueError("BASEROW_API_TOKEN is missing.")

headers = {
    "Authorization": f"Token {API_TOKEN}",
    "Content-Type": "application/json",
}

for table_name, table_id in TABLES.items():
    print(f"\n{'=' * 50}")
    print(f"TABLE: {table_name.upper()}")
    print("=" * 50)

    url = (
        f"https://api.baserow.io/api/database/rows/table/"
        f"{table_id}/?user_field_names=true&size=2"
    )

    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()

    payload = response.json()

    print(f"Total records: {payload.get('count')}")

    for row in payload.get("results", []):
        print(row)