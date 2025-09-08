"""
01_clean_dataset.py
Enhanced data cleaning using smart recovery logic:
- Mathematical recovery of missing values using business relationships
- Category-aware item inference using historical patterns
- Drops only truly unrecoverable rows
- Saves cleaned CSV and dropped rows report
"""

import pandas as pd
import numpy as np
from pathlib import Path

# Paths
ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "dataset" / "retail_store_sales.csv"
CLEAN = ROOT / "dataset" / "retail_store_sales_clean.csv"
DROPPED_ROWS = ROOT / "results" / "dropped_rows_report.csv"
RESULTS = ROOT / "results"
RESULTS.mkdir(exist_ok=True)

log_lines = []
def log(msg: str):
    print(msg)
    log_lines.append(msg)

# Load raw dataset
df = pd.read_csv(RAW)
df_original = df.copy()
log(f"Loaded dataset: {RAW.name} | Shape={df.shape}")

# Snapshot BEFORE cleaning
missing_before = df.isnull().sum()
log(f"Missing values before cleaning: {missing_before.sum()} total")

# Initialize cleaning report
cleaning_report = {
    'initial_rows': len(df),
    'values_recovered': {},
    'values_imputed': {},
    'rows_dropped': 0
}

# --- PHASE 1: IDENTIFY UNRECOVERABLE ROWS ---
log("\n=== PHASE 1: IDENTIFYING UNRECOVERABLE ROWS ===")

# Rows missing both Quantity AND Total Spent are unrecoverable
unrecoverable_mask = df['Quantity'].isna() & df['Total Spent'].isna()
unrecoverable_count = unrecoverable_mask.sum()
log(f"Unrecoverable rows (missing both Quantity & Total): {unrecoverable_count}")

# Save dropped rows report
if unrecoverable_count > 0:
    dropped_rows = df[unrecoverable_mask].copy()
    dropped_rows['Drop_Reason'] = 'Missing both Quantity and Total Spent - mathematically unrecoverable'
    dropped_rows['Has_Price'] = ~dropped_rows['Price Per Unit'].isna()
    dropped_rows['Has_Item'] = ~dropped_rows['Item'].isna()
    dropped_rows['Has_Category'] = ~dropped_rows['Category'].isna()
    
    dropped_rows.to_csv(DROPPED_ROWS, index=False)
    log(f"[OK] Saved {unrecoverable_count} dropped rows to: {DROPPED_ROWS.name}")

# Remove unrecoverable rows
df = df[~unrecoverable_mask].copy()
cleaning_report['rows_dropped'] = unrecoverable_count
log(f"Working with {len(df)} recoverable rows")

# --- PHASE 2: SMART RECOVERY ---
log("\n=== PHASE 2: SMART RECOVERY ===")

# 1. Standardize Discount Applied
log("1. Standardizing Discount Applied...")
if "Discount Applied" in df.columns:
    original_missing = df["Discount Applied"].isna().sum()
    df["Discount Applied"] = df["Discount Applied"].map({
        True: True, False: False, "True": True, "False": False
    })
    df["Discount Applied"] = df["Discount Applied"].fillna(False).astype(bool)
    log(f"[OK] Standardized to boolean; filled {original_missing} missing with False")
    cleaning_report['values_imputed']['discount_applied'] = original_missing

# 2. Recover Price Per Unit using Total / Quantity
log("2. Recovering missing prices using Total / Quantity...")
mask_price = (df['Price Per Unit'].isna() & 
              df['Quantity'].notna() & 
              df['Total Spent'].notna() & 
              (df['Quantity'] > 0))
recovered_prices = mask_price.sum()
if recovered_prices > 0:
    df.loc[mask_price, 'Price Per Unit'] = (
        df.loc[mask_price, 'Total Spent'] / 
        df.loc[mask_price, 'Quantity']
    ).round(2)
    log(f"[OK] Recovered {recovered_prices} prices")
cleaning_report['values_recovered']['prices'] = recovered_prices

# 3. Recover Quantity using Total / Price
log("3. Recovering missing quantities using Total / Price...")
mask_qty = (df['Quantity'].isna() & 
            df['Price Per Unit'].notna() & 
            df['Total Spent'].notna() & 
            (df['Price Per Unit'] > 0))
recovered_qtys = mask_qty.sum()
if recovered_qtys > 0:
    df.loc[mask_qty, 'Quantity'] = (
        df.loc[mask_qty, 'Total Spent'] / 
        df.loc[mask_qty, 'Price Per Unit']
    ).round(1)
    log(f"[OK] Recovered {recovered_qtys} quantities")
cleaning_report['values_recovered']['quantities'] = recovered_qtys

# 4. Recover Total using Price × Quantity
log("4. Recovering missing totals using Price x Quantity...")
mask_total = (df['Total Spent'].isna() & 
              df['Price Per Unit'].notna() & 
              df['Quantity'].notna())
recovered_totals = mask_total.sum()
if recovered_totals > 0:
    df.loc[mask_total, 'Total Spent'] = (
        df.loc[mask_total, 'Price Per Unit'] * 
        df.loc[mask_total, 'Quantity']
    ).round(2)
    log(f"[OK] Recovered {recovered_totals} totals")
cleaning_report['values_recovered']['totals'] = recovered_totals

# 5. Infer missing items using Category + Price matching
log("5. Inferring missing items from Category + Price...")
valid_data = df.dropna(subset=['Category', 'Item', 'Price Per Unit'])
category_items = valid_data.groupby(['Category', 'Item'])['Price Per Unit'].mean().reset_index()

missing_items = df['Item'].isna() & df['Category'].notna() & df['Price Per Unit'].notna()
inferred_items = 0

for idx in df[missing_items].index:
    category = df.loc[idx, 'Category']
    price = df.loc[idx, 'Price Per Unit']
    
    # Find items in same category with similar price (±15% tolerance)
    cat_items = category_items[category_items['Category'] == category]
    price_matches = cat_items[abs(cat_items['Price Per Unit'] - price) <= (price * 0.15)]
    
    if not price_matches.empty:
        # Use closest price match
        closest = price_matches.iloc[(price_matches['Price Per Unit'] - price).abs().argsort()[:1]]
        df.loc[idx, 'Item'] = closest.iloc[0]['Item']
        inferred_items += 1

log(f"[OK] Inferred {inferred_items} items from Category + Price")
cleaning_report['values_recovered']['items'] = inferred_items

# --- PHASE 3: FINAL CALCULATIONS ---
log("\n=== PHASE 3: FINAL CALCULATIONS ===")

# Transaction Date standardization
if "Transaction Date" in df.columns:
    df["Transaction Date"] = pd.to_datetime(df["Transaction Date"], errors="coerce")
    log("[OK] Standardized Transaction Date to datetime")

# Ensure calculation consistency
log("Ensuring calculation consistency...")
calc_mask = df['Price Per Unit'].notna() & df['Quantity'].notna()
calculated_totals = (df.loc[calc_mask, 'Price Per Unit'] * df.loc[calc_mask, 'Quantity']).round(2)

inconsistent_mask = calc_mask & (
    df['Total Spent'].isna() | 
    (abs(df['Total Spent'] - calculated_totals) > 0.01)
)

if inconsistent_mask.any():
    df.loc[inconsistent_mask, 'Total Spent'] = calculated_totals[inconsistent_mask]
    updated_count = inconsistent_mask.sum()
    log(f"[OK] Updated {updated_count} Total Spent values for consistency")

# Final validation - should have no missing critical values
critical_columns = ['Transaction ID', 'Category', 'Item', 'Price Per Unit', 'Quantity', 'Total Spent']
final_missing = df[critical_columns].isnull().sum()
total_missing = final_missing.sum()

if total_missing > 0:
    log(f"WARNING: Still have {total_missing} missing values in critical columns")
else:
    log("[OK] All critical columns now complete!")

# Snapshot AFTER cleaning
missing_after = df.isnull().sum()

# === PHASE 4: SAVE AND REPORT ===
log("\n=== PHASE 4: SAVE AND REPORT ===")

# Save cleaned dataset
df.to_csv(CLEAN, index=False)
log(f"[OK] Saved cleaned dataset: {CLEAN.name} | Shape={df.shape}")

# Generate detailed report
log("\n=== CLEANING SUMMARY ===")
log(f"Initial dataset rows: {cleaning_report['initial_rows']:,}")
log(f"Rows dropped (unrecoverable): {cleaning_report['rows_dropped']:,}")
log(f"Final dataset rows: {len(df):,}")
log(f"Data retention: {(len(df) / cleaning_report['initial_rows'] * 100):.1f}%")

log(f"\nValues RECOVERED (using business logic):")
for key, count in cleaning_report['values_recovered'].items():
    log(f"  {key}: {count:,}")

log(f"\nValues IMPUTED (using defaults):")
for key, count in cleaning_report['values_imputed'].items():
    log(f"  {key}: {count:,}")

log(f"\nMissing values reduction:")
log(f"  Before: {missing_before.sum():,} missing values")
log(f"  After: {missing_after.sum():,} missing values")
log(f"  Reduction: {missing_before.sum() - missing_after.sum():,} values recovered/filled")

# Save log
log_file = RESULTS / "01_cleaning_log.txt"
with open(log_file, "w") as f:
    f.write("\n".join(log_lines))
log(f"[OK] Saved log: {log_file.name}")

# Save quality checks
checks = pd.DataFrame({
    "column": missing_before.index,
    "missing_before": missing_before.values,
    "missing_after": missing_after.values,
    "reduction": missing_before.values - missing_after.values
})
quality_file = RESULTS / "01_quality_checks.csv"
checks.to_csv(quality_file, index=False)
log(f"[OK] Saved quality checks: {quality_file.name}")

print(f"\n[SUCCESS] Enhanced cleaning complete! Dataset ready at: {CLEAN.name}")
print(f"[INFO] Retention rate: {(len(df) / cleaning_report['initial_rows'] * 100):.1f}%")
print(f"[INFO] Dropped rows documented: {DROPPED_ROWS.name}")
print(f"[INFO] Full log available: {log_file.name}")
