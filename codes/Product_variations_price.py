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
# 3) FETCH PRODUCT VARIATIONS FROM BIGBUY
# -----------------------------------------------------------------------------
variations_url = "https://api.bigbuy.eu/rest/catalog/productsvariations.json?isoCode=en&parentTaxonomy=19668"
print("Fetching product variations from BigBuy...")
response = requests.get(variations_url, headers=headers)

if response.status_code == 200:
    variations = response.json()
    print(f"Fetched {len(variations)} product variations. Processing...")
    
    for variation in variations:
        # Extract fields from the JSON response
        variation_id   = variation.get("id")
        product        = variation.get("product")
        sku            = variation.get("sku")
        wholesalePrice = variation.get("wholesalePrice")
        retailPrice    = variation.get("retailPrice")
        inShopsPrice   = variation.get("inShopsPrice")
        extraWeight    = variation.get("extraWeight")
        
        # Insert or update variation data into product_variations_t table
        query = """
        INSERT INTO product_variations_t 
           (id, product, sku, wholesalePrice, retailPrice, inShopsPrice, extraWeight)
        VALUES 
           (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE 
            wholesalePrice = VALUES(wholesalePrice),
            retailPrice = VALUES(retailPrice),
            inShopsPrice = VALUES(inShopsPrice),
            extraWeight = VALUES(extraWeight)
        """
        values = (variation_id, product, sku, wholesalePrice, retailPrice, inShopsPrice, extraWeight)
        cursor.execute(query, values)
        db.commit()
        print(f"Inserted/updated variation {variation_id}")
else:
    print("Failed to fetch product variations:", response.status_code, response.text)

# -----------------------------------------------------------------------------
# 4) CLOSE DATABASE CONNECTION
# -----------------------------------------------------------------------------
cursor.close()
db.close()
print("Product variations processing completed.")
