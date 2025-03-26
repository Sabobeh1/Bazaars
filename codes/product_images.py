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
# 3) FETCH PRODUCT IMAGES FROM BIGBUY
# -----------------------------------------------------------------------------
url = "https://api.bigbuy.eu/rest/catalog/productsimages.json?isoCode=en&parentTaxonomy=19668"
print("Fetching product images from BigBuy...")
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
                is_cover = str(is_cover).upper()  # e.g. convert "true" to "TRUE"
            
            # -----------------------------------------------------------------------------
            # 4) INSERT/UPDATE THE IMAGE DATA INTO products_images_t TABLE
            # -----------------------------------------------------------------------------
            query = """
            INSERT INTO products_images_t (product_id, image_url, is_cover)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE is_cover = VALUES(is_cover)
            """
            values = (product_id, image_url, is_cover)
            cursor.execute(query, values)
    db.commit()
    print("Product images inserted/updated successfully.")
else:
    print("Failed to fetch product images:", response_images.status_code, response_images.text)

# -----------------------------------------------------------------------------
# 5) CLOSE DATABASE CONNECTION
# -----------------------------------------------------------------------------
cursor.close()
db.close()
