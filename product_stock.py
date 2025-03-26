# import requests
# import mysql.connector

# # Database Connection
# db = mysql.connector.connect(
#     host="127.0.0.1",
#     port=3306,
#     user="root",           # <--- Add this line
#     password="aboali4602",
#     database="Bazaars"
# )
# cursor = db.cursor()

# # API Configuration
# stock_url = "https://api.bigbuy.eu/rest/catalog/productsstockbyhandlingdays.json?isoCode=en&parentTaxonomy=19668"
# headers = {"Authorization": "Bearer MGVmNDJiYjRlZTVjYTA0ODM2YzIyYTljZjY3MmFjNzVlYzQ0ZDllMmRhZWYxODA1MTg0MDMzNDY0MGU2ZDI0Zg"}

# # Fetch Stock Data
# try:
#     response_stock = requests.get(stock_url, headers=headers)
#     response_stock.raise_for_status()
#     stocks = response_stock.json()
# except requests.exceptions.RequestException as e:
#     print(f"Error fetching stock data: {e}")
#     stocks = []

# # Process Stock Data
# for stock_entry in stocks:
#     product_id = stock_entry.get("id")
#     stock_list = stock_entry.get("stocks", [])

#     if not product_id or not isinstance(stock_list, list):
#         print(f"Skipping invalid stock entry: {stock_entry}")
#         continue  # Skip bad data

#     total_stock = sum(item.get("quantity", 0) for item in stock_list)

#     query = """
#     INSERT INTO products_stock_t (product_id, stock)
#     VALUES (%s, %s)
#     ON DUPLICATE KEY UPDATE stock = VALUES(stock)
#     """
#     cursor.execute(query, (product_id, total_stock))
#     db.commit()
#     print("hello")

# print("Stock data processed successfully!")

# # Close Database Connection
# cursor.close()
# db.close()
