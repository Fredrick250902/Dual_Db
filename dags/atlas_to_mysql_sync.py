from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from pymongo import MongoClient
from datetime import datetime
import os
import logging

# Fetch Mongo URI from Airflow Environment Variables
MONGO_URI = os.getenv("MONGO_URI")

def parse_date(date_str):
    """
    Converts various date formats to MySQL YYYY-MM-DD format.
    Handles MM/DD/YYYY and YYYY-MM-DD.
    """
    if not date_str or date_str == "N/A":
        return None
    
    for fmt in ('%m/%d/%Y', '%Y-%m-%d', '%d-%m-%Y'):
        try:
            return datetime.strptime(date_str, fmt).strftime('%Y-%m-%d')
        except ValueError:
            continue
    return None

def sync_mongo_to_mysql():
    # --- 1. Connection & Table Setup ---
    mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
    
    # This ensures the table exists even if you manually DROPPED it earlier
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS invoices (
        id INT AUTO_INCREMENT PRIMARY KEY,
        invoice_no VARCHAR(50),
        invoice_date DATE,
        total_amt DECIMAL(10, 2),
        vendor VARCHAR(100),
        synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    mysql_hook.run(create_table_sql)

    # --- 2. Connect to MongoDB ---
    if not MONGO_URI:
        raise ValueError("MONGO_URI environment variable is not set")
    
    m_client = MongoClient(MONGO_URI)
    db = m_client["your_mongodb_name"]
    col = db["your_mongodb_collection_name"]
    
    # Find documents where is_synced is False
    docs = list(col.find({"is_synced": False}))
    
    if not docs:
        logging.info("No new documents found to sync.")
        return

    # --- 3. Sync Logic ---
    success_count = 0
    
    for doc in docs:
        content = doc.get("contents", {})
        
        # Extract and format data
        invoice_no = content.get("invoice_number")
        invoice_date = parse_date(content.get("invoice_date"))
        total_amt = content.get("total_amount", 0.0)
        vendor = content.get("vendor")

        # SQL Insert Query
        sql = """
        INSERT INTO invoices (invoice_no, invoice_date, total_amt, vendor)
        VALUES (%s, %s, %s, %s)
        """
        params = (invoice_no, invoice_date, total_amt, vendor)
        
        try:
            # Execute MySQL Insert
            mysql_hook.run(sql, parameters=params)
            
            # Update MongoDB: Set is_synced to True
            col.update_one(
                {"_id": doc["_id"]}, 
                {"$set": {"is_synced": True}}
            )
            logging.info(f"Successfully synced Invoice {invoice_no} to MySQL.")
            success_count += 1
            
        except Exception as e:
            logging.error(f"Failed to sync document {doc.get('_id')}: {str(e)}")

    logging.info(f"Sync complete. Total records transferred: {success_count}")

# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

with DAG(
    'atlas_to_mysql_sync',
    default_args=default_args,
    description='Syncs unsynced invoice data from MongoDB Atlas to MySQL',
    start_date=datetime(2024, 1, 1),
    schedule_interval='*/5 * * * *',  # Runs every 5 minutes
    catchup=False,
    tags=['mongodb', 'mysql', 'invoice']
) as dag:

    sync_task = PythonOperator(
        task_id='sync_data_task',
        python_callable=sync_mongo_to_mysql
    )
