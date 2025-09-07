"""
03_migrate_to_mysql.py
Loads the CLEANED CSV into MySQL using SQLAlchemy.
- Creates database if it doesn't exist
- Creates/overwrites table retail_sales with proper types
- Adds a primary key & helpful indexes
- Writes logs and schema DDL to results/
"""

import os
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text
from sqlalchemy.types import VARCHAR, DECIMAL, Integer, Date, Boolean
from dotenv import load_dotenv

# ---- paths ----
ROOT = Path(__file__).resolve().parents[1]
CLEAN = ROOT / "dataset" / "retail_store_sales_clean.csv"
RESULTS = ROOT / "results"
RESULTS.mkdir(exist_ok=True)

LOG = RESULTS / "03_mysql_load_log.txt"
DDL = RESULTS / "03_mysql_schema.sql"

def log(msg: str):
    print(msg)
    with open(LOG, "a", encoding="utf-8") as f:
        f.write(msg + "\n")

# ---- load env / config ----
load_dotenv(ROOT / ".env")
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
MYSQL_DB = os.getenv("MYSQL_DB", "retail_db")

# ---- read cleaned data ----
df = pd.read_csv(CLEAN)
log(f"Loaded cleaned CSV: {CLEAN.name} | shape={df.shape}")

# Ensure dtypes (important for stable SQL schema)
# Convert Transaction Date to datetime (if not already), bool stays bool
if "Transaction Date" in df.columns:
    df["Transaction Date"] = pd.to_datetime(df["Transaction Date"], errors="coerce").dt.date
if "Discount Applied" in df.columns:
    # make sure this is strictly bool
    df["Discount Applied"] = df["Discount Applied"].astype(bool)

# ---- connect to MySQL (server level, no DB yet) ----
def mysql_url(db_name=None):
    db = f"/{db_name}" if db_name else ""
    pwd = f":{MYSQL_PASSWORD}" if MYSQL_PASSWORD else ""
    return f"mysql+pymysql://{MYSQL_USER}{pwd}@{MYSQL_HOST}:{MYSQL_PORT}{db}?charset=utf8mb4"

server_engine = create_engine(mysql_url(), pool_pre_ping=True)

# Create DB if not exists
with server_engine.connect() as conn:
    conn.execute(text(f"CREATE DATABASE IF NOT EXISTS `{MYSQL_DB}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"))
    log(f"Ensured database exists: {MYSQL_DB}")

# ---- connect to the target DB ----
engine = create_engine(mysql_url(MYSQL_DB), pool_pre_ping=True)

table_name = "retail_sales"

# Define SQL types for each column
dtype_map = {
    "Transaction ID": VARCHAR(32),
    "Customer ID": VARCHAR(32),
    "Category": VARCHAR(64),
    "Item": VARCHAR(64),
    "Price Per Unit": DECIMAL(10, 2),
    "Quantity": Integer(),
    "Total Spent": DECIMAL(10, 2),
    "Payment Method": VARCHAR(32),
    "Location": VARCHAR(32),
    "Transaction Date": Date(),
    "Discount Applied": Boolean(create_constraint=False),  # in MySQL becomes TINYINT(1)
}

# Write the table (overwrite each run for reproducibility)
log("Writing DataFrame to MySQL (this may take a moment)...")
df.to_sql(
    table_name,
    engine,
    if_exists="replace",
    index=False,
    dtype=dtype_map
)
log(f"Written table: {table_name}")

# Add primary key & indexes (needs separate DDL after to_sql)
ddl_statements = [
    f"ALTER TABLE `{table_name}` ADD PRIMARY KEY (`Transaction ID`);",
    f"CREATE INDEX idx_customer ON `{table_name}` (`Customer ID`);",
    f"CREATE INDEX idx_date ON `{table_name}` (`Transaction Date`);",
    f"CREATE INDEX idx_category ON `{table_name}` (`Category`);"
]

with engine.begin() as conn:
    for stmt in ddl_statements:
        try:
            conn.execute(text(stmt))
            log(f"OK: {stmt}")
        except Exception as e:
            log(f"SKIP/ERR: {stmt} -> {e}")

# Save DDL we used (good for your report appendix)
with open(DDL, "w", encoding="utf-8") as f:
    f.write(f"-- Schema & Indexes for {table_name}\n")
    for stmt in ddl_statements:
        f.write(stmt + "\n")
log(f"[saved] {DDL}")

# Row count check from DB
with engine.connect() as conn:
    count = conn.execute(text(f"SELECT COUNT(*) FROM `{table_name}`;")).scalar()
    log(f"Row count in MySQL `{table_name}`: {count}")

log("MySQL migration complete.")
print(f"[saved] {LOG}")
