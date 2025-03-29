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
cursor = db.cursor(dictionary=True)

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
taxonomy_url = "https://api.bigbuy.eu/rest/catalog/taxonomies.json?firstLevel&isoCode=en"
print("Fetching categories from BigBuy...")
taxonomy_response = requests.get(taxonomy_url, headers=headers)

if taxonomy_response.status_code == 200:
    all_taxonomies = taxonomy_response.json()
    print(f"Fetched {len(all_taxonomies)} categories. Inserting/Updating in DB...")
    
    for tax in all_taxonomies:
        cat_id    = tax.get("id")
        cat_name  = tax.get("name")
        parent_tax = tax.get("parentTaxonomy")
        dateAdd   = tax.get("dateAdd")   # e.g., "2021-10-20 13:53:25"
        dateUpd   = tax.get("dateUpd")   # e.g., "2023-03-01 16:18:08"
        urlImages = tax.get("urlImages")
        isoCode   = tax.get("isoCode")
        
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
# 3.5) FETCH FIRST 2 CATEGORIES FROM THE DATABASE FOR TESTING
# -----------------------------------------------------------------------------
print("\nFetching first 2 categories from the database for product fetching test...")
cursor.execute("SELECT id, name FROM categories ORDER BY id LIMIT 4")
first_two_categories = cursor.fetchall()
if not first_two_categories:
    print("No categories found in the database. Exiting.")
    cursor.close()
    db.close()
    exit()

# -----------------------------------------------------------------------------
# 4) FOR EACH OF THE FIRST 2 CATEGORIES, FETCH PRODUCTS AND INSERT INTO products_t
# -----------------------------------------------------------------------------
for cat in first_two_categories:
    cat_id = cat["id"]
    cat_name = cat["name"]
    # Build dynamic product endpoint with parentTaxonomy equal to the category id
    product_url = f"https://api.bigbuy.eu/rest/catalog/products.json?isoCode=en&parentTaxonomy={cat_id}"
    print(f"\nFetching products for category '{cat_name}' (ID: {cat_id}) from {product_url}...")
    response = requests.get(product_url, headers=headers)
    
    if response.status_code == 200:
        products = response.json()
        print(f"Fetched {len(products)} products for category '{cat_name}'. Processing...")
        
        insert_count = 0
        skip_count = 0
        
        for product in products:
            product_condition = product.get("condition")
            print(f"Product ID: {product.get('id')} -> condition: {product_condition}")
            if product_condition != "NEW":
                skip_count += 1
                continue

            product_id = product.get("id")
            sku = product.get("sku")
            weight = product.get("weight")
            category_id = product.get("category")  # This should match a value in the categories table
            
            # Optionally, check if the referenced category exists.
            cursor.execute("SELECT COUNT(*) AS cnt FROM categories WHERE id = %s", (category_id,))
            cat_exists = cursor.fetchone()["cnt"]
            if cat_exists == 0:
                print(f"Warning: Category Sub_Level ID {category_id} does not exist in 'categories' table for product {product_id}")
                # Decide whether to skip; here, we'll still insert.
            
            query = """
            INSERT IGNORE INTO products_t (id, sku, weight, category, product_condition, categoryTopLevelID)
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            values = (product_id, sku, weight, category_id, product_condition, cat_id)
            cursor.execute(query, values)
            db.commit()
            insert_count += 1
        
        print(f"Category '{cat_name}': Inserted/updated {insert_count} NEW products; Skipped {skip_count} products.")
    else:
        print(f"Failed to fetch products for category {cat_id}: {response.status_code} {response.text}")

# -----------------------------------------------------------------------------
# 5) LOOP THROUGH EACH CATEGORY, FETCH PRODUCT INFORMATION, AND INSERT INTO products_info_t
# -----------------------------------------------------------------------------

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
# 6) FOR EACH CATEGORY, FETCH PRODUCT IMAGES USING A DYNAMIC API ENDPOINT,
#    THEN INSERT/UPDATE INTO products_images_t
# -----------------------------------------------------------------------------

# Build the dynamic API endpoint with the category id
    url = f"https://api.bigbuy.eu/rest/catalog/productsimages.json?isoCode=en&parentTaxonomy={cat_id}"
    print(f"\nFetching product images for category '{cat_name}' (ID: {cat_id}) from {url}...")
    response_images = requests.get(url, headers=headers)
    
    if response_images.status_code == 200:
        image_entries = response_images.json()
        for entry in image_entries:
            product_id = entry.get("id")
            images_list = entry.get("images", [])
            for image in images_list:
                image_url = image.get("url")
                # Ensure is_cover is stored as a string ("TRUE" or "FALSE")
                is_cover = image.get("isCover")
                if isinstance(is_cover, bool):
                    is_cover = "TRUE" if is_cover else "FALSE"
                else:
                    is_cover = str(is_cover).upper()
                
                query = """
                INSERT INTO products_images_t (product_id, image_url, is_cover)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE is_cover = VALUES(is_cover)
                """
                values = (product_id, image_url, is_cover)
                cursor.execute(query, values)
        db.commit()
        print(f"Product images inserted/updated successfully for category '{cat_name}'.")
    else:
        print(f"Failed to fetch product images for category {cat_id}: {response_images.status_code} {response_images.text}")

# -----------------------------------------------------------------------------
# 7) CLOSE DATABASE CONNECTION
# -----------------------------------------------------------------------------
cursor.close()
db.close()
print("\nAll done.")
