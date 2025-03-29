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

# Output files for the export (this export will be combined for all categories processed)
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
# Function: Process one category (update variations, update stock, and export CSV for that category)
# ------------------------------
def process_category(cat):
    cat_id = cat["id"]
    cat_name = cat["name"]
    print(f"\nProcessing category '{cat_name}' (ID: {cat_id})...")
    
    # ------------------------------
    # Update product variations for this category
    # ------------------------------
    variations_url = f"https://api.bigbuy.eu/rest/catalog/productsvariations.json?isoCode=en&parentTaxonomy={cat_id}"
    print(f"Fetching product variations for category '{cat_name}' from {variations_url}...")
    try:
        db_var = mysql.connector.connect(**DB_CONFIG)
        cursor_var = db_var.cursor()
        response_var = requests.get(variations_url, headers=HEADERS)
        if response_var.status_code == 200:
            variations = response_var.json()
            print(f"Fetched {len(variations)} product variations for category '{cat_name}'. Processing...")
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
                cursor_var.execute(query, values)
                db_var.commit()
                print(f"Inserted/updated variation {variation_id}")
        else:
            print(f"Failed to fetch product variations for category {cat_id}: {response_var.status_code} {response_var.text}")
    except Error as e:
        print("Database error in update_product_variations:", e)
    finally:
        if db_var.is_connected():
            cursor_var.close()
            db_var.close()
            print("Product variations update for category completed.")

    # ------------------------------
    # Update product variations stock for this category
    # ------------------------------
    try:
        db_stock = mysql.connector.connect(**DB_CONFIG)
        cursor_stock = db_stock.cursor()
        stock_url = f"https://api.bigbuy.eu/rest/catalog/productsvariationsstockbyhandlingdays.json?isoCode=en&parentTaxonomy={cat_id}"
        print(f"Fetching product variations stock for category '{cat_name}' from {stock_url}...")
        response_stock = requests.get(stock_url, headers=HEADERS)
        if response_stock.status_code == 200:
            stocks = response_stock.json()
            print(f"Fetched {len(stocks)} stock entries for category '{cat_name}'. Processing...")
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
                cursor_stock.execute(query, (variation_id, total_stock))
                db_stock.commit()
                print(f"Updated stock for variation {variation_id}: {total_stock}")
        else:
            print(f"Failed to fetch stock for category {cat_id}: {response_stock.status_code} {response_stock.text}")
    except Error as e:
        print("Database error in update_product_variations_stock:", e)
    finally:
        if db_stock.is_connected():
            cursor_stock.close()
            db_stock.close()
            print("Product variations stock update for category completed.")

    # ------------------------------
    # Export final CSV for this category (filtered by p.category = cat_id)
    # ------------------------------
    final_category_data = []
    try:
        db_export = mysql.connector.connect(**DB_CONFIG)
        cursor_export = db_export.cursor(dictionary=True)
        export_query = f"""
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
        LEFT JOIN categories c ON p.categoryTopLevelID = c.id
        LEFT JOIN product_variations_t pv ON p.id = pv.product
        LEFT JOIN product_variations_stock_t pvs ON pv.id = pvs.id
        WHERE p.product_condition = 'NEW'
          AND p.categoryTopLevelID = {cat_id}
        GROUP BY p.id
        """
        print(f"Executing export query for category '{cat_name}'...")
        cursor_export.execute(export_query)
        results = cursor_export.fetchall()
        print(f"Fetched {len(results)} rows for category '{cat_name}'.")
        
        for row in results:
            for key, value in row.items():
                if isinstance(value, Decimal):
                    row[key] = float(value)
            try:
                images_list = json.loads(row['item_images'])
            except Exception:
                images_list = []
            row['item_images'] = images_list
            final_category_data.append(row)
        
        cursor_export.close()
        db_export.close()
    except Error as e:
        print("Database error in export for category:", e)
    
    # Return the final data for this category
    return final_category_data

# ------------------------------
# Task: Export final CSV for all categories (by iterating through categories)
# ------------------------------
def export_all_categories_csv():
    try:
        db = mysql.connector.connect(**DB_CONFIG)
        cursor = db.cursor(dictionary=True)
        print("Fetching all categories for export...")
        # For testing, we use LIMIT 2; remove LIMIT to process all categories.
        cursor.execute("SELECT id, name FROM categories ORDER BY id LIMIT 4")
        categories = cursor.fetchall()
        if not categories:
            print("No categories found. Exiting export.")
            cursor.close()
            db.close()
            return
        
        combined_data = []
        for cat in categories:
            cat_data = process_category(cat)
            combined_data.extend(cat_data)
        
        print(f"Combined data from all categories: {len(combined_data)} rows.")
        
        # Save JSON
        print(f"Saving JSON data to {JSON_OUTPUT_FILE}...")
        with open(JSON_OUTPUT_FILE, 'w', encoding='utf-8') as json_file:
            json.dump(combined_data, json_file, indent=2, default=convert_decimal_to_float)
        print("JSON file saved.")
        
        # Save CSV
        print(f"Saving CSV data to {CSV_OUTPUT_FILE}...")
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
            for item in combined_data:
                if isinstance(item["item_images"], list):
                    item["item_images"] = "[" + ", ".join(f'"{img}"' for img in item["item_images"]) + "]"
                writer.writerow(item)
        print(f"CSV file saved as {CSV_OUTPUT_FILE}.")
        
        cursor.close()
        db.close()
    except Error as e:
        print(f"Database error in export_all_categories_csv: {e}")

# ------------------------------
# Job: Run the export for all categories (iterating over categories)
# ------------------------------
def job():
    print("Starting refresh cycle for all categories...")
    export_all_categories_csv()
    print("Refresh cycle completed.\n")

# ------------------------------
# Schedule the job to run every 15 minutes (set to 2 minutes for testing)
# ------------------------------
import schedule
schedule.every(12).minutes.do(job)

print("Scheduler started. Running tasks every 15 minutes...")
while True:
    schedule.run_pending()
    time.sleep(1)