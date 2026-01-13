import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    'host': os.getenv('DB_HOST', '127.0.0.1'),
    'port': os.getenv('DB_PORT', '5433'),
    'dbname': os.getenv('DB_NAME', 'aggr_data'),
    'user': os.getenv('DB_USER', 'aggr_user'),
    'password': os.getenv('DB_PASSWORD'),
}

try:
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("SELECT 1")
    print("Database connection successful!")
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    tables = cur.fetchall()
    print(f"Tables found: {[t[0] for t in tables]}")
    conn.close()
except Exception as e:
    print(f"Database connection failed: {e}")
