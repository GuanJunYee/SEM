"""
03_migrate_to_mysql.py
Loads the cleaned dataset into MySQL.
"""

import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, text

# ==== CONFIG ====
CSV_PATH = "dataset/retail_store_sales_clean.csv"
DB_NAME = "retail_db"
TABLE_NAME = "sales"

# Update with your MySQL credentials
MYSQL_USER = "root"
MYSQL_PASS = "password"      # <-- change to your MySQL password
MYSQL_HOST = "localhost"
MYSQL_PORT = "3306"

# ==== STEP 1: Load cleaned dataset ====
df = pd.read_csv(CSV_PATH)

# ==== STEP 2: Connect to MySQL server ====
engine = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASS}@{MYSQL_HOST}:{MYSQL_PORT}/")

with engine.connect() as conn:
    conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}"))
    conn.execute(text(f"USE {DB_NAME}"))

# Reconnect into the new DB
engine = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASS}@{MYSQL_HOST}:{MYSQL_PORT}/{DB_NAME}")

# ==== STEP 3: Write DataFrame to MySQL table ====
df.to_sql(TABLE_NAME, con=engine, if_exists="replace", index=False)

print(f"[DONE] Loaded {len(df)} rows into MySQL table `{DB_NAME}.{TABLE_NAME}`")
