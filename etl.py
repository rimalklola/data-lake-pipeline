import duckdb
import psycopg2
import random
import json
import os
from datetime import datetime, timedelta
from google.cloud import storage  # <--- The Official GCP Library

# --- CONFIGURATION ---
PG_CONN = "dbname=production_db user=user password=password host=localhost"
STATE_FILE = "pipeline_state.json"

# --- GCP CONFIG ---
# 1. Point to the key you downloaded (Must be in same folder)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "gcp_credentials.json"
# 2. Your Bucket Name (Change this to YOUR bucket name)
GCS_BUCKET_NAME = "rim-data-lake-2025" 

def generate_mock_data():
    """Generates fake sales data in Postgres"""
    print("--- STEP 1: Generating Mock Data in Postgres ---")
    try:
        conn = psycopg2.connect(PG_CONN)
        cur = conn.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS orders (id SERIAL PRIMARY KEY, amount FLOAT, created_at TIMESTAMP);")
        
        data = []
        base_time = datetime.now()
        # Generate 5,000 rows
        for _ in range(5000):
            rand_days = random.randint(0, 10)
            ts = base_time - timedelta(days=rand_days)
            amount = round(random.uniform(10.0, 500.0), 2)
            data.append((amount, ts))
        
        args_str = ','.join(cur.mogrify("(%s,%s)", x).decode('utf-8') for x in data)
        cur.execute("INSERT INTO orders (amount, created_at) VALUES " + args_str)
        conn.commit()
        cur.close()
        conn.close()
        print(" -> Success: Inserted 5,000 rows into Postgres.")
    except Exception as e:
        print(f" -> Error generating data: {e}")

def get_last_watermark():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            return json.load(f).get('last_run', "1970-01-01 00:00:00")
    return "1970-01-01 00:00:00"

def upload_to_gcs(local_file, destination_blob_name):
    """
    Uploads a file to the bucket using the Service Account
    """
    print(f" -> Uploading {local_file} to GCS...")
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(destination_blob_name)

        blob.upload_from_filename(local_file)
        print(f" -> Upload Success: gs://{GCS_BUCKET_NAME}/{destination_blob_name}")
        return True
    except Exception as e:
        print(f" -> Upload FAILED: {e}")
        return False

def run_etl():
    print("\n--- STEP 2: Starting Incremental ETL Job (GCP Edition) ---")
    
    last_run = get_last_watermark()
    print(f" -> High Watermark: {last_run}")

    con = duckdb.connect()
    con.execute("INSTALL postgres; LOAD postgres;")
    con.execute(f"ATTACH '{PG_CONN}' AS pg (TYPE POSTGRES);")

    # We extract data to a LOCAL parquet file first (Staging Area)
    # Naming convention includes timestamp to avoid collisions
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    local_filename = f"orders_{timestamp_str}.parquet"
    
    query = f"""
        SELECT * FROM pg.orders 
        WHERE created_at > TIMESTAMP '{last_run}'
    """
    
    print(f" -> Extracting new rows to local stage: {local_filename}...")
    
    # 1. Extract to Local Disk
    con.execute(f"COPY ({query}) TO '{local_filename}' (FORMAT PARQUET);")
    
    # 2. Calculate the 'Partition' paths for GCS
    # In a real scenario, you might split this file into multiple partitions.
    # For this 'Mini' project, we upload the whole file to the current day's folder.
    current_year = datetime.now().year
    current_month = datetime.now().month
    current_day = datetime.now().day
    
    # Hive-Style Partition Path: year=2025/month=11/day=27/file.parquet
    gcs_path = f"raw_orders/year={current_year}/month={current_month}/day={current_day}/{local_filename}"
    
    # 3. Upload to Cloud
    if upload_to_gcs(local_filename, gcs_path):
        # 4. Cleanup (Delete local file only if upload worked)
        if os.path.exists(local_filename):
            os.remove(local_filename)
            print(" -> Local cleanup complete.")

        # 5. Update State
        new_watermark = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(STATE_FILE, 'w') as f:
            json.dump({"last_run": new_watermark}, f)
        
        print(f" -> Pipeline Finished. New Watermark: {new_watermark}")

if __name__ == "__main__":
    generate_mock_data()
    run_etl()