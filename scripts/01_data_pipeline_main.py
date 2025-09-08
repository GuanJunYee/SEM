"""
01_data_pipeline_main.py
Data Processing Pipeline with Smart Recovery Logic
Clean → Normalize → MySQL (default) + MongoDB (with denormalization option)
- Mathematical recovery of missing values using business relationships
- Category-aware item inference using historical patterns
- Drops only truly unrecoverable rows
- Comprehensive tracking and reporting
"""

import os
import sys
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

# Set pandas options to avoid warnings
pd.set_option('future.no_silent_downcasting', True)
import argparse

# Setup paths
ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "dataset"
NORMALIZED_DIR = DATA_DIR / "normalized"  # Folder for normalized CSV files
RESULTS_DIR = ROOT / "results"
RESULTS_DIR.mkdir(exist_ok=True)
NORMALIZED_DIR.mkdir(exist_ok=True)  # Create normalized folder

# Input/Output files
RAW_CSV = DATA_DIR / "retail_store_sales.csv"
CLEAN_CSV = DATA_DIR / "retail_store_sales_clean.csv"
DROPPED_ROWS_CSV = RESULTS_DIR / "01_dropped_rows_report.csv"
LOG_FILE = RESULTS_DIR / "01_pipeline.log"

log_lines = []

def log(message):
    """Log with timestamp and save to file"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_msg = f"[{timestamp}] {message}"
    print(log_msg)
    log_lines.append(log_msg)

def clean_data():
    """Step 1: Data cleaning with smart recovery logic"""
    log("STEP 1: DATA CLEANING WITH SMART RECOVERY")
    log("=" * 60)
    
    # Load raw data
    df = pd.read_csv(RAW_CSV)
    df_original = df.copy()
    log(f"Loaded raw data: {df.shape} records")
    
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
    
    # Remove rows with critical missing identifiers first
    critical_missing = df['Transaction ID'].isna() | df['Customer ID'].isna()
    critical_count = critical_missing.sum()
    if critical_count > 0:
        df = df[~critical_missing].copy()
        log(f"Removed {critical_count} rows with missing Transaction/Customer ID")
        cleaning_report['rows_dropped'] += critical_count
    
    # Rows missing both Quantity AND Total Spent are mathematically unrecoverable
    unrecoverable_mask = df['Quantity'].isna() & df['Total Spent'].isna()
    unrecoverable_count = unrecoverable_mask.sum()
    log(f"Unrecoverable rows (missing both Quantity & Total): {unrecoverable_count}")
    
    # Save dropped rows report
    total_dropped = critical_count + unrecoverable_count
    if total_dropped > 0:
        # Combine critical and unrecoverable rows for report
        all_dropped = []
        
        if critical_count > 0:
            critical_rows = df_original[critical_missing].copy()
            critical_rows['Drop_Reason'] = 'Missing critical identifiers (Transaction ID or Customer ID)'
            all_dropped.append(critical_rows)
        
        if unrecoverable_count > 0:
            unrecoverable_rows = df[unrecoverable_mask].copy()
            unrecoverable_rows['Drop_Reason'] = 'Missing both Quantity and Total Spent - mathematically unrecoverable'
            unrecoverable_rows['Has_Price'] = ~unrecoverable_rows['Price Per Unit'].isna()
            unrecoverable_rows['Has_Item'] = ~unrecoverable_rows['Item'].isna()
            unrecoverable_rows['Has_Category'] = ~unrecoverable_rows['Category'].isna()
            all_dropped.append(unrecoverable_rows)
        
        if all_dropped:
            dropped_df = pd.concat(all_dropped, ignore_index=True)
            dropped_df.to_csv(DROPPED_ROWS_CSV, index=False)
            log(f"[OK] Saved {len(dropped_df)} dropped rows to: {DROPPED_ROWS_CSV.name}")
    
    # Remove unrecoverable rows
    df = df[~unrecoverable_mask].copy()
    cleaning_report['rows_dropped'] += unrecoverable_count
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
    
    # 6. Final cleanup - remove remaining rows with missing items
    remaining_missing_items = df['Item'].isna().sum()
    if remaining_missing_items > 0:
        df = df.dropna(subset=['Item'])
        log(f"[CLEANUP] Removed {remaining_missing_items} rows with unrecoverable missing items")
        cleaning_report['rows_dropped'] += remaining_missing_items
    
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
        for col, count in final_missing.items():
            if count > 0:
                log(f"  {col}: {count} missing")
    else:
        log("[OK] All critical columns now complete!")
    
    # Snapshot AFTER cleaning
    missing_after = df.isnull().sum()
    
    # Save cleaned data
    df.to_csv(CLEAN_CSV, index=False)
    log(f"[OK] Saved cleaned data: {CLEAN_CSV.name} | Shape={df.shape}")
    
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
    
    return df

def normalize_data(df):
    """Step 2: Create normalized tables"""
    log("\nSTEP 2: DATA NORMALIZATION")
    log("=" * 40)
    
    normalized_tables = {}
    
    # Categories table
    categories = df['Category'].unique()
    categories_df = pd.DataFrame({
        'CategoryID': range(1, len(categories) + 1),
        'CategoryName': sorted(categories)
    })
    categories_df.to_csv(NORMALIZED_DIR / "categories.csv", index=False)
    normalized_tables['categories'] = categories_df
    log(f"Created Categories table: {len(categories_df)} records → saved to normalized/categories.csv")
    
    # Customers table  
    customers = df['Customer ID'].unique()
    customers_df = pd.DataFrame({
        'CustomerID': customers,
        'CustomerName': [f"Customer_{cid.split('_')[1]}" for cid in customers]
    })
    customers_df.to_csv(NORMALIZED_DIR / "customers.csv", index=False)
    normalized_tables['customers'] = customers_df
    log(f"Created Customers table: {len(customers_df)} records → saved to normalized/customers.csv")
    
    # Payment Methods table
    payment_methods = df['Payment Method'].unique()
    payment_methods_df = pd.DataFrame({
        'PaymentMethodID': range(1, len(payment_methods) + 1),
        'PaymentMethodName': sorted(payment_methods)
    })
    payment_methods_df.to_csv(NORMALIZED_DIR / "payment_methods.csv", index=False)
    normalized_tables['payment_methods'] = payment_methods_df
    log(f"Created PaymentMethods table: {len(payment_methods_df)} records → saved to normalized/payment_methods.csv")
    
    # Locations table
    locations = df['Location'].unique()
    locations_df = pd.DataFrame({
        'LocationID': range(1, len(locations) + 1),
        'LocationName': sorted(locations)
    })
    locations_df.to_csv(NORMALIZED_DIR / "locations.csv", index=False)
    normalized_tables['locations'] = locations_df
    log(f"Created Locations table: {len(locations_df)} records → saved to normalized/locations.csv")
    
    # Items table
    items_unique = df[['Item', 'Category', 'Price Per Unit']].drop_duplicates()
    category_map = dict(zip(categories_df['CategoryName'], categories_df['CategoryID']))
    items_df = pd.DataFrame({
        'ItemID': range(1, len(items_unique) + 1),
        'ItemName': items_unique['Item'].values,
        'PricePerUnit': items_unique['Price Per Unit'].values,
        'CategoryID': items_unique['Category'].map(category_map).values
    })
    items_df.to_csv(NORMALIZED_DIR / "items.csv", index=False)
    normalized_tables['items'] = items_df
    log(f"Created Items table: {len(items_df)} records → saved to normalized/items.csv")
    
    # Transactions table (normalized)
    payment_map = dict(zip(payment_methods_df['PaymentMethodName'], payment_methods_df['PaymentMethodID']))
    location_map = dict(zip(locations_df['LocationName'], locations_df['LocationID']))
    
    # Create item mapping
    item_category_key = items_df['ItemName'] + '|' + items_df['CategoryID'].astype(str)
    item_map = dict(zip(item_category_key, items_df['ItemID']))
    df_item_key = df['Item'] + '|' + df['Category'].map(category_map).astype(str)
    
    transactions_df = pd.DataFrame({
        'TransactionID': df['Transaction ID'],
        'CustomerID': df['Customer ID'],
        'ItemID': df_item_key.map(item_map),
        'PaymentMethodID': df['Payment Method'].map(payment_map),
        'LocationID': df['Location'].map(location_map),
        'Quantity': df['Quantity'],
        'TotalPrice': df['Total Spent'],
        'TransactionDate': df['Transaction Date'],
        'DiscountApplied': df['Discount Applied']
    })
    transactions_df.to_csv(NORMALIZED_DIR / "transactions_normalized.csv", index=False)
    normalized_tables['transactions'] = transactions_df
    log(f"Created Transactions table: {len(transactions_df)} records → saved to normalized/transactions_normalized.csv")
    
    log(f"\nNormalization complete! Created {len(normalized_tables)} tables")
    return normalized_tables

def denormalize_for_mongo(normalized_tables):
    """Step 3b: Denormalize data for MongoDB (optional)"""
    log("\nSTEP 3b: DENORMALIZATION FOR MONGODB")
    log("=" * 40)
    
    # Join all tables back into a flat structure for MongoDB
    transactions = normalized_tables['transactions']
    categories = normalized_tables['categories']
    customers = normalized_tables['customers']
    payment_methods = normalized_tables['payment_methods']
    locations = normalized_tables['locations']
    items = normalized_tables['items']
    
    # Merge to create denormalized view
    denormalized = transactions.merge(
        customers, left_on='CustomerID', right_on='CustomerID', how='left'
    ).merge(
        items, left_on='ItemID', right_on='ItemID', how='left'
    ).merge(
        categories, left_on='CategoryID', right_on='CategoryID', how='left'
    ).merge(
        payment_methods, left_on='PaymentMethodID', right_on='PaymentMethodID', how='left'
    ).merge(
        locations, left_on='LocationID', right_on='LocationID', how='left'
    )
    
    # Select relevant columns for MongoDB
    mongo_df = denormalized[[
        'TransactionID', 'CustomerName', 'CategoryName', 'ItemName', 
        'PricePerUnit', 'Quantity', 'TotalPrice', 'PaymentMethodName',
        'LocationName', 'TransactionDate', 'DiscountApplied'
    ]].copy()
    
    mongo_df.to_csv(NORMALIZED_DIR / "mongo_denormalized.csv", index=False)
    log(f"Created denormalized data for MongoDB: {len(mongo_df)} records → saved to normalized/mongo_denormalized.csv")
    
    return mongo_df

def main():
    """Data pipeline with smart recovery logic"""
    parser = argparse.ArgumentParser(description="Data Pipeline with Smart Recovery")
    parser.add_argument('--denormalize-mongo', action='store_true', 
                       help='Create denormalized data for MongoDB')
    parser.add_argument('--skip-cleaning', action='store_true',
                       help='Skip cleaning step (use existing clean data)')
    
    args = parser.parse_args()
    
    log("DATA PIPELINE WITH SMART RECOVERY")
    log("=" * 60)
    
    try:
        # Step 1: Clean data (unless skipped)
        if args.skip_cleaning and CLEAN_CSV.exists():
            log("Skipping cleaning - using existing clean data")
            df_clean = pd.read_csv(CLEAN_CSV)
        else:
            df_clean = clean_data()
        
        # Step 2: Normalize data (default behavior)
        normalized_tables = normalize_data(df_clean)
        
        # Step 3a: Data is now ready for MySQL (normalized)
        log(f"\n>> READY FOR MYSQL: Normalized tables saved to {DATA_DIR}")
        log("   Use 02_mysql_migration_normalized.py to load into MySQL")
        
        # Step 3b: Optionally denormalize for MongoDB
        if args.denormalize_mongo:
            denormalize_for_mongo(normalized_tables)
            log(f"\n>> READY FOR MONGODB: Denormalized data saved to {DATA_DIR}")
            log("   Use 03_mongodb_migration_normalized.py --denormalized to load into MongoDB")
        else:
            log(f"\n>> MongoDB: Use --denormalize-mongo flag to create denormalized data")
        
        log(f"\n>> SUCCESS: Pipeline completed!")
        log(f"   Log saved to: {LOG_FILE}")
        log(f"   Dropped rows report: {DROPPED_ROWS_CSV}")
        
        # Save complete log to file
        with open(LOG_FILE, "w", encoding="utf-8") as f:
            for line in log_lines:
                f.write(line + "\n")
        
    except Exception as e:
        log(f"ERROR: {str(e)}")
        # Save log even if there's an error
        with open(LOG_FILE, "w", encoding="utf-8") as f:
            for line in log_lines:
                f.write(line + "\n")
        sys.exit(1)

if __name__ == "__main__":
    main()
