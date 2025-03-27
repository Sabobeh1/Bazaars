import json
import csv
import mysql.connector
from mysql.connector import Error
from decimal import Decimal

# -----------------------------------------------------------------------------
# Configuration: MySQL credentials and output file names
# -----------------------------------------------------------------------------
DB_CONFIG = {
    'host': '127.0.0.1',
    'user': 'root',
    'port': '3306',
    'password': 'aboali4602',
    'database': 'bazaars2'
}

JSON_OUTPUT_FILE = 'final_products_export.json'
CSV_OUTPUT_FILE = 'final_products_export.csv'

# -----------------------------------------------------------------------------
# Helper: Convert Decimal to float for JSON serialization
# -----------------------------------------------------------------------------
def convert_decimal_to_float(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError

# -----------------------------------------------------------------------------
# Main logic
# -----------------------------------------------------------------------------
def main():
    try:
        print("Connecting to MySQL...")
        connection = mysql.connector.connect(**DB_CONFIG)
        cursor = connection.cursor(dictionary=True)
        
        # -----------------------------------------------------------------------------
        # Run SQL query to join tables and map fields:
        #
        # Mapping:
        #   title             -> products_info_t.title
        #   actual_price      -> product_variations_t.inShopsPrice
        #   approved          -> constant "yes"
        #   item_description  -> products_info_t.item_description
        #   item_category     -> IFNULL(c.name, CONCAT('CatID:', p.category))
        #   item_images       -> aggregated from products_images_t (by product_id)
        #   item_stock        -> product_variations_stock_t.stock (joined on product_variations_t.id)
        #   price             -> product_variations_t.retailPrice
        #   product_weight    -> products_t.weight
        #   sale_price        -> product_variations_t.wholesalePrice
        #
        # For this example, we assume each product (in products_t) has one associated variation.
        # -----------------------------------------------------------------------------
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
        
        print("Executing query to fetch final product data...")
        cursor.execute(query)
        results = cursor.fetchall()
        print(f"Fetched {len(results)} rows.")
        
        # -----------------------------------------------------------------------------
        # Convert any Decimal fields to float
        # -----------------------------------------------------------------------------
        for row in results:
            for key, value in row.items():
                if isinstance(value, Decimal):
                    row[key] = float(value)
        
        # -----------------------------------------------------------------------------
        # Transform the item_images field:
        # The query returns a JSON array string; convert it to a Python list.
        # -----------------------------------------------------------------------------
        final_data = []
        for row in results:
            try:
                images_list = json.loads(row['item_images'])
            except Exception as e:
                images_list = []
            row['item_images'] = images_list
            final_data.append(row)
        
        print(f"Transformed {len(final_data)} rows to final format.")
        
        # -----------------------------------------------------------------------------
        # Save to JSON file
        # -----------------------------------------------------------------------------
        print(f"Saving JSON data to {JSON_OUTPUT_FILE}...")
        with open(JSON_OUTPUT_FILE, 'w', encoding='utf-8') as json_file:
            json.dump(final_data, json_file, indent=2, default=convert_decimal_to_float)
        print("JSON file saved.")
        
        # -----------------------------------------------------------------------------
        # Save to CSV file
        # -----------------------------------------------------------------------------
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
            
            for item in final_data:
                # Convert the item_images list to a string formatted as, for example: ["url1", "url2"]
                if isinstance(item["item_images"], list):
                    item["item_images"] = "[" + ", ".join(f'"{img}"' for img in item["item_images"]) + "]"
                writer.writerow(item)
        
        print("CSV file saved.")
        
    except Error as e:
        print(f"Database error: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("Database connection closed.")

# -----------------------------------------------------------------------------
# Run the script
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    main()
