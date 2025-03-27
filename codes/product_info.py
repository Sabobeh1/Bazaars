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
cursor = db.cursor(dictionary=True)

# -----------------------------------------------------------------------------
# 2) DEFINE AUTH HEADERS FOR BIGBUY
# -----------------------------------------------------------------------------
headers = {
    "Authorization": "Bearer MGVmNDJiYjRlZTVjYTA0ODM2YzIyYTljZjY3MmFjNzVlYzQ0ZDllMmRhZWYxODA1MTg0MDMzNDY0MGU2ZDI0Zg"
}

# -----------------------------------------------------------------------------
# 3) FETCH FIRST 2 CATEGORIES FROM THE DATABASE
# -----------------------------------------------------------------------------
print("Fetching first 2 categories from the database for product info test...")
cursor.execute("SELECT id, name FROM categories ORDER BY id LIMIT 2")
first_two_categories = cursor.fetchall()

if not first_two_categories:
    print("No categories found in the database. Exiting.")
    cursor.close()
    db.close()
    exit()

# -----------------------------------------------------------------------------
# 4) LOOP THROUGH EACH CATEGORY, FETCH PRODUCT INFORMATION, AND INSERT INTO products_info_t
# -----------------------------------------------------------------------------
for category in first_two_categories:
    cat_id = category["id"]
    cat_name = category["name"]
    # Build the dynamic API endpoint using the category id
    url = f"https://api.bigbuy.eu/rest/catalog/productsinformation.json?isoCode=en&parentTaxonomy={cat_id}"
    print(f"\nFetching product information for category '{cat_name}' (ID: {cat_id}) from {url}...")
    response_info = requests.get(url, headers=headers)
    
    if response_info.status_code == 200:
        product_infos = response_info.json()
        print(f"Fetched {len(product_infos)} product information entries for category '{cat_name}'. Processing...")
        
        for info in product_infos:
            product_id = info.get("id")
            sku = info.get("sku")
            # Map the BigBuy "name" field to our "title" field
            title = info.get("name")
            # Map the BigBuy "description" field to our "description" column
            description = info.get("description")
            
            # Check if product info for this product already exists
            cursor.execute("SELECT COUNT(*) AS cnt FROM products_info_t WHERE id = %s", (product_id,))
            if cursor.fetchone()["cnt"] > 0:
                print(f"Product with ID {product_id} already exists in products_info_t. Skipping.")
                continue
            
            query = """
            INSERT INTO products_info_t (id, sku, title, description)
            VALUES (%s, %s, %s, %s)
            """
            values = (product_id, sku, title, description)
            cursor.execute(query, values)
            db.commit()
            print(f"Inserted product info for product ID {product_id}")
    else:
        print(f"Failed to fetch product information for category ID {cat_id}: {response_info.status_code} {response_info.text}")

# -----------------------------------------------------------------------------
# 5) CLOSE DATABASE CONNECTION
# -----------------------------------------------------------------------------
cursor.close()
db.close()
print("\nAll done.")
