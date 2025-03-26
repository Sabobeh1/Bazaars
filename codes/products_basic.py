import sys
sys.stdout.reconfigure(encoding='utf-8')

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
# 3) FETCH AND INSERT TAXONOMIES (CATEGORIES) INTO `categories` TABLE
#    Using GET /rest/catalog/taxonomies.json?firstLevel to get only first-level categories.
# -----------------------------------------------------------------------------
taxonomy_url = "https://api.bigbuy.eu/rest/catalog/taxonomies.json?firstLevel"
print("Fetching categories from BigBuy...")
taxonomy_response = requests.get(taxonomy_url, headers=headers)

if taxonomy_response.status_code == 200:
    all_taxonomies = taxonomy_response.json()
    print(f"Fetched {len(all_taxonomies)} categories. Inserting/Updating in DB...")
    
    for tax in all_taxonomies:
        cat_id = tax.get("id")
        cat_name = tax.get("name")
        parent_tax = tax.get("parentTaxonomy")
        dateAdd = tax.get("dateAdd")   # e.g., "2021-10-20 13:53:25"
        dateUpd = tax.get("dateUpd")   # e.g., "2023-03-01 16:18:08"
        urlImages = tax.get("urlImages")
        isoCode = tax.get("isoCode")
        
        query = """
        INSERT IGNORE INTO categories (id, name, parentTaxonomy, dateAdd, dateUpd, urlImages, isoCode)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        values = (cat_id, cat_name, parent_tax, dateAdd, dateUpd, urlImages, isoCode)
        cursor.execute(query, values)
        print(f"Inserted category {cat_id} - {cat_name}")
    
    db.commit()
    print("Categories inserted/updated successfully.")
else:
    print("Failed to fetch categories:", taxonomy_response.status_code, taxonomy_response.text)

# -----------------------------------------------------------------------------
# 4) FETCH PRODUCTS FROM BIGBUY (using the products.json endpoint)
#    And INSERT INTO `products_t` (only base product data)
# -----------------------------------------------------------------------------
product_url = "https://api.bigbuy.eu/rest/catalog/products.json?isoCode=en&parentTaxonomy=19668"
print(f"\nFetching products from {product_url}...")
response = requests.get(product_url, headers=headers)

if response.status_code == 200:
    products = response.json()
    print(f"Fetched {len(products)} products. Processing...")
    
    insert_count = 0
    skip_count = 0
    
    for product in products:
        # Retrieve the product condition; process only if it is "NEW"
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
            # Depending on your logic, you can skip insertion or still insert.
        
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
    print("Failed to fetch products:", response.status_code, response.text)

# -----------------------------------------------------------------------------
# 5) CLOSE DATABASE CONNECTION
# -----------------------------------------------------------------------------
cursor.close()
db.close()
print("\nAll done.")
