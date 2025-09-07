"""
04_migrate_to_mongo.py
Reads from MySQL (retail_db.retail_sales) and writes to MongoDB.
Writes a log with counts to results/.
"""

import os
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text
from pymongo import MongoClient, InsertOne
from dotenv import load_dotenv

# --- paths ---
ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "results"
RESULTS.mkdir(exist_ok=True)
LOG = RESULTS / "04_mongo_load_log.txt"

def log(msg: str):
    print(msg)
    with open(LOG, "a", encoding="utf-8") as f:
        f.write(msg + "\n")

# --- config ---
load_dotenv(ROOT / ".env")

# MySQL
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
MYSQL_DB = os.getenv("MYSQL_DB", "retail_db")

def mysql_url(db):
    pwd = f":{MYSQL_PASSWORD}" if MYSQL_PASSWORD else ""
    return f"mysql+pymysql://{MYSQL_USER}{pwd}@{MYSQL_HOST}:{MYSQL_PORT}/{db}?charset=utf8mb4"

# Mongo
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "retail_store")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "sales")

# --- read from MySQL ---
engine = create_engine(mysql_url(MYSQL_DB), pool_pre_ping=True)
table = "retail_sales"

with engine.connect() as conn:
    mysql_count = conn.execute(text(f"SELECT COUNT(*) FROM `{table}`")).scalar()
log(f"MySQL row count in `{table}`: {mysql_count}")

df = pd.read_sql(f"SELECT * FROM `{table}`", engine)

# Convert date to Python datetime for Mongo (if needed)
if "Transaction Date" in df.columns:
    df["Transaction Date"] = pd.to_datetime(df["Transaction Date"], errors="coerce")

# --- write to Mongo ---
client = MongoClient(MONGO_URI)
mdb = client[MONGO_DB]
col = mdb[MONGO_COLLECTION]

# Replace collection for reproducibility
col.drop()
log(f"Dropped existing collection `{MONGO_COLLECTION}` in DB `{MONGO_DB}` (if existed).")

# Bulk insert for speed
ops = [InsertOne(rec) for rec in df.to_dict(orient="records")]
if ops:
    result = col.bulk_write(ops, ordered=False)
    log(f"Inserted docs: {result.inserted_count}")

mongo_count = col.count_documents({})
log(f"MongoDB row count in `{MONGO_DB}.{MONGO_COLLECTION}`: {mongo_count}")

# Sanity: list distinct Payment Method values
try:
    pm = col.distinct("Payment Method")
    log(f"Distinct Payment Method in Mongo: {pm}")
except Exception as e:
    log(f"Distinct fetch failed: {e}")

log("Mongo migration complete.")
print(f"[saved] {LOG}")
