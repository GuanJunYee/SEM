"""
05_validate_migration.py
Cross-checks MySQL vs MongoDB:
- row count equality
- min/max Transaction Date
- distinct category/payment counts
- spot-check presence by Transaction ID
Writes results to results/05_validation_report.txt
"""

import os
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text
from pymongo import MongoClient
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "results"
RESULTS.mkdir(exist_ok=True)
REPORT = RESULTS / "05_validation_report.txt"

def write(line: str):
    print(line)
    with open(REPORT, "a", encoding="utf-8") as f:
        f.write(line + "\n")

load_dotenv(ROOT / ".env")

# MySQL config
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
MYSQL_DB = os.getenv("MYSQL_DB", "retail_db")

def mysql_url(db):
    pwd = f":{MYSQL_PASSWORD}" if MYSQL_PASSWORD else ""
    return f"mysql+pymysql://{MYSQL_USER}{pwd}@{MYSQL_HOST}:{MYSQL_PORT}/{db}?charset=utf8mb4"

# Mongo config
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "retail_store")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "sales")

# --- MySQL pulls ---
engine = create_engine(mysql_url(MYSQL_DB), pool_pre_ping=True)
table = "retail_sales"

with engine.connect() as conn:
    mysql_count = conn.execute(text(f"SELECT COUNT(*) FROM `{table}`")).scalar()
    min_date = conn.execute(text(f"SELECT MIN(`Transaction Date`) FROM `{table}`")).scalar()
    max_date = conn.execute(text(f"SELECT MAX(`Transaction Date`) FROM `{table}`")).scalar()
    n_cat = conn.execute(text(f"SELECT COUNT(DISTINCT `Category`) FROM `{table}`")).scalar()
    n_pm = conn.execute(text(f"SELECT COUNT(DISTINCT `Payment Method`) FROM `{table}`")).scalar()
    sample_ids = pd.read_sql(text(f"SELECT `Transaction ID` FROM `{table}` LIMIT 5"), conn)["Transaction ID"].tolist()

# --- Mongo pulls ---
client = MongoClient(MONGO_URI)
col = client[MONGO_DB][MONGO_COLLECTION]
mongo_count = col.count_documents({})

mongo_min = col.aggregate([{"$group": {"_id": None, "min": {"$min": "$Transaction Date"}}}])
mongo_min_date = next(mongo_min, {}).get("min")
mongo_max = col.aggregate([{"$group": {"_id": None, "max": {"$max": "$Transaction Date"}}}])
mongo_max_date = next(mongo_max, {}).get("max")
mongo_n_cat = len(col.distinct("Category"))
mongo_n_pm = len(col.distinct("Payment Method"))

# --- Write report ---
write("== Validation Report ==")
write(f"MySQL rows: {mysql_count}")
write(f"MongoDB rows: {mongo_count}")
write(f"Row count match: {mysql_count == mongo_count}")

write(f"\nDate range (MySQL): {min_date} → {max_date}")
write(f"Date range (Mongo): {mongo_min_date} → {mongo_max_date}")

write(f"\nDistinct categories: MySQL={n_cat} | Mongo={mongo_n_cat}")
write(f"Distinct payment methods: MySQL={n_pm} | Mongo={mongo_n_pm}")

# Spot-check: are sample IDs present in Mongo?
missing = []
for tid in sample_ids:
    if col.count_documents({"Transaction ID": tid}, limit=1) == 0:
        missing.append(tid)
write(f"\nSample spot-check (5 IDs) missing in Mongo: {missing if missing else 'None'}")

write("\nValidation complete.")
print(f"[saved] {REPORT}")
