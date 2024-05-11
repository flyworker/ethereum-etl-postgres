import csv
import os
from datetime import datetime
import logging
import psycopg2
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


# Set up logging to file and console
logger = logging.getLogger('DBImportLogger')
logger.setLevel(logging.INFO)

# File handler
file_handler = logging.FileHandler('import_log.log')
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

# Console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

# Add handlers to the logger
logger.addHandler(file_handler)
logger.addHandler(console_handler)
# Database connection parameters from .env file
dbname = os.getenv('DB_NAME')
user = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
host = os.getenv('DB_HOST')
# Database connection parameters

csv_file_path = '../../../Downloads/QmddQyCmN72Yxc4SzXHMkH2ryQyragTgrnWFYD8tu2VWBp.csv'
# Connect to the PostgreSQL database
conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host)
cur = conn.cursor()


# Function to process large CSV file in batches
def process_csv_in_batches(csv_file_path, batch_size=1000):
    try:
        with open(csv_file_path, 'r') as f:
            reader = csv.DictReader(f)
            batch = []
            total_rows = 0  # Track the total number of rows processed
            for row in reader:
                # Convert UNIX timestamp and handle empty fields
                row['block_timestamp'] = datetime.utcfromtimestamp(int(row['block_timestamp'])).strftime('%Y-%m-%d %H:%M:%S') if row['block_timestamp'] else None
                for key in ['nonce', 'transaction_index', 'block_number', 'gas', 'gas_price', 'max_fee_per_gas', 'max_priority_fee_per_gas', 'transaction_type', 'max_fee_per_blob_gas']:
                    row[key] = None if row[key] == '' else row[key]
                batch.append(tuple(row.values()))
                if len(batch) >= batch_size:
                    try:
                        cur.executemany(insert_query, batch)
                        conn.commit()
                        total_rows += len(batch)
                        logger.info("Committed a batch of %d rows, total rows committed: %d", len(batch), total_rows)
                    except psycopg2.IntegrityError as e:
                        conn.rollback()
                        logger.error("Duplicate key error, batch skipped: %s", e)
                    batch = []
            if batch:  # Commit any remaining rows in the last batch
                try:
                    cur.executemany(insert_query, batch)
                    conn.commit()
                    total_rows += len(batch)
                    logger.info("Committed the final batch of %d rows, total rows committed: %d", len(batch), total_rows)
                except psycopg2.IntegrityError as e:
                    conn.rollback()
                    logger.error("Duplicate key error, final batch skipped: %s", e)
            logger.info("All data inserted successfully from %s", csv_file_path)
    except Exception as e:
        logger.error("Error loading data from CSV: %s", e)
        conn.rollback()
    finally:
        cur.close()
        conn.close()
        logger.info("Database connection closed")

# INSERT statement template (modify based on your specific column and table names)
insert_query = """
INSERT INTO transactions (
    hash, nonce, block_hash, block_number, transaction_index, 
    from_address, to_address, value, gas, gas_price, input, 
    block_timestamp, max_fee_per_gas, max_priority_fee_per_gas, 
    transaction_type, max_fee_per_blob_gas, blob_versioned_hashes
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

# Path to your CSV file


# Process CSV in batches
process_csv_in_batches(csv_file_path)
