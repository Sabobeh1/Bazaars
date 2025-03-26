import requests
import mysql.connector
import json

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
# 2) DEFINE AUTH HEADERS FOR BIGBUY
# -----------------------------------------------------------------------------
headers = {
    "Authorization": "Bearer MGVmNDJiYjRlZTVjYTA0ODM2YzIyYTljZjY3MmFjNzVlYzQ0ZDllMmRhZWYxODA1MTg0MDMzNDY0MGU2ZDI0Zg"
}

# -----------------------------------------------------------------------------
# 3) FETCH PRODUCT INFORMATION FROM BIGBUY
#    (This endpoint returns product name and description among other fields)
# -----------------------------------------------------------------------------
url = "https://api.bigbuy.eu/rest/catalog/productsinformation.json?isoCode=en&parentTaxonomy=19668"
response_info = requests.get(url, headers=headers)

if response_info.status_code == 200:
    product_infos = response_info.json()
    for info in product_infos:
        product_id = info.get("id")
        sku = info.get("sku")
        # Map the BigBuy "name" field to our "title" field
        title = info.get("name")
        # Map the BigBuy "description" field to our "item_description" field
        item_description = info.get("description")
        
        # Check if product with the same id already exists in products_info_t
        cursor.execute("SELECT COUNT(*) FROM products_info_t WHERE id = %s", (product_id,))
        if cursor.fetchone()[0] > 0:
            print(f"Product with ID {product_id} already exists. Skipping.")
            continue
        
        query = """
        INSERT INTO products_info_t (id, sku, title, item_description)
        VALUES (%s, %s, %s, %s)
        """
        values = (product_id, sku, title, item_description)
        cursor.execute(query, values)
        db.commit()
        print(f"Inserted product info for product ID {product_id}")
else:
    print("Failed to fetch data:", response_info.status_code, response_info.text)

cursor.close()
db.close()
