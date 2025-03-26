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
# 3) FETCH PRODUCTS FROM BIGBUY (using the products.json endpoint)
# -----------------------------------------------------------------------------
url = "https://api.bigbuy.eu/rest/catalog/products.json?isoCode=en&parentTaxonomy=19668"
print(f"Fetching products from {url}...")
response = requests.get(url, headers=headers)

if response.status_code == 200:
    products = response.json()
    print(f"Fetched {len(products)} products. Processing...")
    
    insert_count = 0
    skip_count = 0
    
    for product in products:
        # Retrieve the product condition; only process products where condition is "NEW"
        product_condition = product.get("condition")
        print(f"Product ID: {product.get('id')} -> condition: {product_condition}")
        if product_condition != "NEW":
            skip_count += 1
            continue

        # Extract required fields for products_t
        product_id = product.get("id")
        sku = product.get("sku")
        weight = product.get("weight")
        category_id = product.get("category")  # This should match a value in the categories table
        
        # Optionally, check if the referenced category exists in the categories table.
        cursor.execute("SELECT COUNT(*) FROM categories WHERE id = %s", (category_id,))
        cat_exists = cursor.fetchone()[0]
        if cat_exists == 0:
            print(f"Warning: Category ID {category_id} does not exist in 'categories' table for product {product_id}")
            # Depending on your logic, you can choose to skip or still insert the product.
            # For now, we'll still insert.
        
        # Insert the product data into products_t
        query = """
        INSERT IGNORE INTO products_t (id, sku, weight, category, product_condition)
        VALUES (%s, %s, %s, %s, %s)
        """
        values = (product_id, sku, weight, category_id, product_condition)
        cursor.execute(query, values)
        db.commit()
        insert_count += 1
    
    print(f"Inserted/updated {insert_count} NEW products.")
    print(f"Skipped {skip_count} products that were not NEW.")
else:
    print("Failed to fetch data:", response.status_code, response.text)

cursor.close()
db.close()
print("\nAll done.")