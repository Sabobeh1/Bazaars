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
# 3) FETCH FIRST 2 CATEGORIES FROM THE DATABASE FOR TESTING
# -----------------------------------------------------------------------------
print("Fetching first 2 categories from the database for testing...")
cursor.execute("SELECT id, name FROM categories ORDER BY id LIMIT 2")
first_two_categories = cursor.fetchall()
if not first_two_categories:
    print("No categories found in the database. Exiting.")
    cursor.close()
    db.close()
    exit()

# -----------------------------------------------------------------------------
# 4) FOR EACH CATEGORY, FETCH PRODUCT IMAGES USING A DYNAMIC API ENDPOINT,
#    THEN INSERT/UPDATE INTO products_images_t
# -----------------------------------------------------------------------------
for category in first_two_categories:
    cat_id = category["id"]
    cat_name = category["name"]
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
# 5) CLOSE DATABASE CONNECTION
# -----------------------------------------------------------------------------
cursor.close()
db.close()
print("\nAll done.")
