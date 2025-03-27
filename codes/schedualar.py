import schedule
import time
import requests
import mysql.connector
import json
import csv
from mysql.connector import Error
from decimal import Decimal

# ------------------------------
# Database configuration
# ------------------------------
DB_CONFIG = {
    'host': '127.0.0.1',
    'user': 'root',
    'port': 3306,
    'password': 'aboali4602',
    'database': 'bazaars2'
}

# Authorization headers for BigBuy
HEADERS = {
    "Authorization": "Bearer MGVmNDJiYjRlZTVjYTA0ODM2YzIyYTljZjY3MmFjNzVlYzQ0ZDllMmRhZWYxODA1MTg0MDMzNDY0MGU2ZDI0Zg"
}

# Output files for the export
JSON_OUTPUT_FILE = 'final_products_export.json'
CSV_OUTPUT_FILE = 'final_products_export.csv'

# ------------------------------
# Helper: Convert Decimal to float
# ------------------------------
def convert_decimal_to_float(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError

# ------------------------------
# Task 1: Update product variations (prices)
# ------------------------------
def update_product_variations():
    try:
        db = mysql.connector.connect(**DB_CONFIG)
        cursor = db.cursor()
        variations_url = "https://api.bigbuy.eu/rest/catalog/productsvariations.json?isoCode=en&parentTaxonomy=19668"
        print("Fetching product variations from BigBuy...")
        response = requests.get(variations_url, headers=HEADERS)
        if response.status_code == 200:
            variations = response.json()
            print(f"Fetched {len(variations)} product variations. Processing...")
            for variation in variations:
                variation_id   = variation.get("id")
                product        = variation.get("product")
                sku            = variation.get("sku")
                wholesalePrice = variation.get("wholesalePrice")
                retailPrice    = variation.get("retailPrice")
                inShopsPrice   = variation.get("inShopsPrice")
                extraWeight    = variation.get("extraWeight")
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
    except Error as e:
        print("Database error in update_product_variations:", e)
    finally:
        if db.is_connected():
            cursor.close()
            db.close()
            print("Product variations update: Database connection closed.")

# ------------------------------
# Task 2: Update product variations stock
# ------------------------------
def update_product_variations_stock():
    try:
        db = mysql.connector.connect(**DB_CONFIG)
        cursor = db.cursor()
        stock_url = "https://api.bigbuy.eu/rest/catalog/productsvariationsstockbyhandlingdays.json?isoCode=en&parentTaxonomy=19668"
        print("Fetching product variations stock data from BigBuy...")
        response = requests.get(stock_url, headers=HEADERS)
        if response.status_code == 200:
            stocks = response.json()
            print(f"Fetched {len(stocks)} stock entries. Processing...")
            for stock_entry in stocks:
                variation_id = stock_entry.get("id")
                stock_list = stock_entry.get("stocks", [])
                if not variation_id or not isinstance(stock_list, list):
                    print(f"Skipping invalid stock entry: {stock_entry}")
                    continue
                total_stock = sum(item.get("quantity", 0) for item in stock_list)
                query = """
                INSERT INTO product_variations_stock_t (id, stock)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE stock = VALUES(stock)
                """
                cursor.execute(query, (variation_id, total_stock))
                db.commit()
                print(f"Updated stock for variation {variation_id}: {total_stock}")
        else:
            print("Failed to fetch product variations stock:", response.status_code, response.text)
    except Error as e:
        print("Database error in update_product_variations_stock:", e)
    finally:
        if db.is_connected():
            cursor.close()
            db.close()
            print("Product variations stock update: Database connection closed.")

# ------------------------------
# Task 3: Export final CSV (joining static & dynamic data)
# ------------------------------
def export_final_csv():
    try:
        db = mysql.connector.connect(**DB_CONFIG)
        cursor = db.cursor(dictionary=True)
        print("Executing query to fetch final product data for export...")
        query = """
        SELECT 
            pi.title AS title,
            ANY_VALUE(pv.inShopsPrice) AS actual_price,
            'yes' AS approved,
            pi.description AS item_description,
            IFNULL(c.name, CONCAT('CatID:', p.category)) AS item_category,
            COALESCE(
                (SELECT JSON_ARRAYAGG(image_url)
                 FROM products_images_t
                 WHERE product_id = p.id),
                JSON_ARRAY()
            ) AS item_images,
            ANY_VALUE(pvs.stock) AS item_stock,
            ANY_VALUE(pv.retailPrice) AS price,
            p.weight AS product_weight,
            ANY_VALUE(pv.wholesalePrice) AS sale_price
        FROM products_t p
        LEFT JOIN products_info_t pi ON p.id = pi.id
        LEFT JOIN categories c ON p.category = c.id
        LEFT JOIN product_variations_t pv ON p.id = pv.product
        LEFT JOIN product_variations_stock_t pvs ON pv.id = pvs.id
        WHERE p.product_condition = 'NEW'
        GROUP BY p.id
        """
        cursor.execute(query)
        results = cursor.fetchall()
        print(f"Fetched {len(results)} rows.")
        
        # Convert any Decimal fields to float
        for row in results:
            for key, value in row.items():
                if isinstance(value, Decimal):
                    row[key] = float(value)
        
        # Transform item_images field (convert JSON string to list)
        final_data = []
        for row in results:
            try:
                images_list = json.loads(row['item_images'])
            except Exception as e:
                images_list = []
            row['item_images'] = images_list
            final_data.append(row)
        
        print(f"Transformed {len(final_data)} rows to final format.")
        
        # Save JSON export (optional)
        with open(JSON_OUTPUT_FILE, 'w', encoding='utf-8') as json_file:
            json.dump(final_data, json_file, indent=2, default=convert_decimal_to_float)
        print(f"JSON file saved as {JSON_OUTPUT_FILE}.")
        
        # Save CSV export
        field_names = [
            "title",
            "actual_price",
            "approved",
            "item_description",
            "item_category",
            "item_images",
            "item_stock",
            "price",
            "product_weight",
            "sale_price"
        ]
        with open(CSV_OUTPUT_FILE, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=field_names)
            writer.writeheader()
            for item in final_data:
                if isinstance(item["item_images"], list):
                    item["item_images"] = "[" + ", ".join(f'"{img}"' for img in item["item_images"]) + "]"
                writer.writerow(item)
        print(f"CSV file saved as {CSV_OUTPUT_FILE}.")
        
    except Error as e:
        print(f"Database error in export_final_csv: {e}")
    finally:
        if db.is_connected():
            cursor.close()
            db.close()
            print("Export final CSV: Database connection closed.")

# ------------------------------
# Job: Run all three tasks
# ------------------------------
def job():
    print("Refreshing product variations, stock, and CSV export...")
    update_product_variations()
    update_product_variations_stock()
    export_final_csv()
    print("Refresh cycle completed.\n")

# ------------------------------
# Schedule the job to run every 15 minutes
# ------------------------------
import schedule

schedule.every(2).minutes.do(job)

print("Scheduler started. Running tasks every 15 minutes...")
while True:
    schedule.run_pending()
    time.sleep(1)
