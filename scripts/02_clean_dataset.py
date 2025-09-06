"""
02_clean_dataset.py
Cleans the dataset and saves:
- Cleaned CSV into dataset/
- Cleaning log + quality checks into results/
"""

import pandas as pd
from pathlib import Path

# Paths
ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "dataset" / "retail_store_sales.csv"
CLEAN = ROOT / "dataset" / "retail_store_sales_clean.csv"
RESULTS = ROOT / "results"
RESULTS.mkdir(exist_ok=True)

log_lines = []
def log(msg: str):
    print(msg)
    log_lines.append(msg)

# Load raw dataset
df = pd.read_csv(RAW)
log(f"Loaded dataset: {RAW.name} | Shape={df.shape}")

# Snapshot BEFORE cleaning
missing_before = df.isnull().sum()

# --- 1) Standardize types first ---
# Transaction Date -> datetime (coerce bad formats to NaT)
if "Transaction Date" in df.columns:
    df["Transaction Date"] = pd.to_datetime(df["Transaction Date"], errors="coerce")
    log("Cast 'Transaction Date' -> datetime")

# Discount Applied -> boolean (normalize strings, fill missing as False)
if "Discount Applied" in df.columns:
    df["Discount Applied"] = df["Discount Applied"].map(
        {True: True, False: False, "True": True, "False": False}
    )
    df["Discount Applied"] = df["Discount Applied"].fillna(False).astype(bool)
    log("Standardized 'Discount Applied' to boolean; filled missing with False")

# --- 2) Categorical imputation ---
if "Item" in df.columns:
    n_missing = df["Item"].isna().sum()
    df["Item"] = df["Item"].fillna("Unknown")
    log(f"Filled 'Item' missing: {n_missing} -> 'Unknown'")

# --- 3) Numeric imputation (median) ---
for col in ["Price Per Unit", "Quantity"]:
    if col in df.columns:
        nulls = df[col].isna().sum()
        if nulls:
            med = df[col].median()
            df[col] = df[col].fillna(med)
            log(f"Filled '{col}' missing: {nulls} with median={med}")

# --- 4) Recompute derived column ---
# Total Spent = Price Per Unit * Quantity (authoritative recompute)
if {"Price Per Unit", "Quantity"}.issubset(df.columns):
    df["Total Spent"] = (df["Price Per Unit"] * df["Quantity"]).round(2)
    log("Recomputed 'Total Spent' = Price Per Unit × Quantity (rounded 2dp)")

# --- 5) Light sanity constraints (optional, non-destructive checks) ---
# Ensure Quantity and Price are > 0
if "Quantity" in df.columns and (df["Quantity"] <= 0).any():
    bad = int((df["Quantity"] <= 0).sum())
    log(f"WARNING: {bad} rows have non-positive Quantity (not changed).")
if "Price Per Unit" in df.columns and (df["Price Per Unit"] <= 0).any():
    bad = int((df["Price Per Unit"] <= 0).sum())
    log(f"WARNING: {bad} rows have non-positive Price Per Unit (not changed).")

# Snapshot AFTER cleaning
missing_after = df.isnull().sum()

# Save cleaned dataset
df.to_csv(CLEAN, index=False)
log(f"Saved cleaned dataset: {CLEAN.name} | Shape={df.shape}")

# --- Write logs & checks to results/ ---
log_path = RESULTS / "02_cleaning_log.txt"
with open(log_path, "w", encoding="utf-8") as f:
    f.write("== Cleaning Log ==\n")
    f.write("\n".join(log_lines))
    f.write("\n\n== Missing Values (BEFORE) ==\n")
    f.write(missing_before.to_string())
    f.write("\n\n== Missing Values (AFTER) ==\n")
    f.write(missing_after.to_string())
print(f"[saved] {log_path}")

# Quality checklist
quality = {
    "no_nulls_Item": (df["Item"].isna().sum() == 0) if "Item" in df.columns else "n/a",
    "no_nulls_PricePerUnit": (df["Price Per Unit"].isna().sum() == 0) if "Price Per Unit" in df.columns else "n/a",
    "no_nulls_Quantity": (df["Quantity"].isna().sum() == 0) if "Quantity" in df.columns else "n/a",
    "no_nulls_TotalSpent": (df["Total Spent"].isna().sum() == 0) if "Total Spent" in df.columns else "n/a",
    "DiscountApplied_is_bool": df["Discount Applied"].map(lambda x: isinstance(x, (bool,))).all() if "Discount Applied" in df.columns else "n/a",
    "TransactionDate_is_datetime": pd.api.types.is_datetime64_any_dtype(df["Transaction Date"]) if "Transaction Date" in df.columns else "n/a",
}
pd.Series(quality, name="result").to_csv(RESULTS / "02_quality_checks.csv")
print(f"[saved] {RESULTS / '02_quality_checks.csv'}")
