import requests
import mysql.connector

# -----------------------------------------------------------------------------
# 1) CONNECT TO DATABASE
# -----------------------------------------------------------------------------
db = mysql.connector.connect(
    host="127.0.0.1",
    port=3306,
    user="root",
    password="aboali4602",
    database="bazaars2"
)
cursor = db.cursor()

# -----------------------------------------------------------------------------
# 2) API CONFIGURATION FOR STOCK VARIATION
# -----------------------------------------------------------------------------
# Using the stock variation endpoint for products with variations.
stock_url = "https://api.bigbuy.eu/rest/catalog/productsvariationsstockbyhandlingdays.json?isoCode=en&parentTaxonomy=19668"
headers = {
    "Authorization": "Bearer MGVmNDJiYjRlZTVjYTA0ODM2YzIyYTljZjY3MmFjNzVlYzQ0ZDllMmRhZWYxODA1MTg0MDMzNDY0MGU2ZDI0Zg"
}

# -----------------------------------------------------------------------------
# 3) FETCH STOCK DATA FROM BIGBUY
# -----------------------------------------------------------------------------
try:
    response_stock = requests.get(stock_url, headers=headers)
    response_stock.raise_for_status()
    stocks = response_stock.json()
except requests.exceptions.RequestException as e:
    print(f"Error fetching stock data: {e}")
    stocks = []

# -----------------------------------------------------------------------------
# 4) PROCESS AND INSERT/UPDATE STOCK DATA
# -----------------------------------------------------------------------------
for stock_entry in stocks:
    # For variations, the "id" field is the variation ID
    variation_id = stock_entry.get("id")
    stock_list = stock_entry.get("stocks", [])

    if not variation_id or not isinstance(stock_list, list):
        print(f"Skipping invalid stock entry: {stock_entry}")
        continue

    # Sum up the stock from all handling days for the variation
    total_stock = sum(item.get("quantity", 0) for item in stock_list)

    query = """
    INSERT INTO product_variations_stock_t (id, stock)
    VALUES (%s, %s)
    ON DUPLICATE KEY UPDATE stock = VALUES(stock)
    """
    cursor.execute(query, (variation_id, total_stock))
    db.commit()
    print(f"Updated stock for variation {variation_id}: {total_stock}")

print("Stock data processed successfully!")

# -----------------------------------------------------------------------------
# 5) CLOSE DATABASE CONNECTION
# -----------------------------------------------------------------------------
cursor.close()
db.close()
