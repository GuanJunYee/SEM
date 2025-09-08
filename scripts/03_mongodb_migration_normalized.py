"""
03_mongodb_migration_normalized.py
Migrates data from normalized MySQL structure to MongoDB.
Supports both normalized and denormalized document structures:

1. Normalized: Separate collections mirroring MySQL tables
   - categories, locations, payment_methods, customers, items, transactions

2. Denormalized: Embedded documents for better MongoDB performance
   - customers, transactions_with_details (with embedded item, category, payment, location data)
"""

import os
import argparse
from pathlib import Path
from datetime import datetime
from sqlalchemy import create_engine, text
from pymongo import MongoClient
from dotenv import load_dotenv
import pandas as pd

# ---- Setup paths ----
ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "results"
RESULTS.mkdir(exist_ok=True)

LOG = RESULTS / "03_mongo_normalized_load_log.txt"

def log(msg: str):
    print(msg)
    with open(LOG, "a", encoding="utf-8") as f:
        f.write(f"{datetime.now()}: {msg}\n")

# ---- Load environment config ----
load_dotenv(ROOT / ".env")

# MySQL config
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
MYSQL_DB = os.getenv("MYSQL_DB", "retail_db_normalized")

# MongoDB config
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "retail_store_normalized")

def mysql_url(db_name):
    """Create MySQL connection URL"""
    pwd = f":{MYSQL_PASSWORD}" if MYSQL_PASSWORD else ""
    return f"mysql+pymysql://{MYSQL_USER}{pwd}@{MYSQL_HOST}:{MYSQL_PORT}/{db_name}?charset=utf8mb4"

def validate_connections():
    """Validate both MySQL and MongoDB connections"""
    log("Validating database connections...")
    
    try:
        # Test MySQL connection
        mysql_engine = create_engine(mysql_url(MYSQL_DB), pool_pre_ping=True)
        with mysql_engine.connect() as conn:
            result = conn.execute(text("SELECT 1")).scalar()
            if result == 1:
                log("[OK] MySQL connection validated")
            else:
                raise Exception("MySQL connection test failed")
        
        # Test MongoDB connection
        mongo_client = MongoClient(MONGO_URI)
        mongo_client.server_info()  # This will raise an exception if connection fails
        log("[OK] MongoDB connection validated")
        
        return mysql_engine, mongo_client
        
    except Exception as e:
        log(f"ERROR: Connection validation failed: {e}")
        raise

def get_mysql_counts(engine):
    """Get record counts from MySQL tables"""
    log("Getting MySQL record counts...")
    
    counts = {}
    tables = ["Categories", "Locations", "PaymentMethods", "Customers", "Items", "Transactions"]
    
    with engine.connect() as conn:
        for table in tables:
            try:
                count = conn.execute(text(f"SELECT COUNT(*) FROM `{table}`")).scalar()
                counts[table.lower()] = count
                log(f"  {table}: {count:,} records")
            except Exception as e:
                log(f"  ERROR counting {table}: {e}")
                counts[table.lower()] = 0
    
    return counts

def clear_mongodb_collections(client, denormalize=False):
    """Clear existing MongoDB collections"""
    log("Clearing existing MongoDB collections...")
    
    db = client[MONGO_DB]
    
    if denormalize:
        # Denormalized structure collections
        collections = ["customers", "transactions_with_details"]
    else:
        # Normalized structure collections
        collections = ["categories", "locations", "payment_methods", "customers", "items", "transactions"]
    
    for collection_name in collections:
        try:
            db[collection_name].drop()
            log(f"  Dropped collection: {collection_name}")
        except Exception as e:
            log(f"  Could not drop {collection_name}: {e}")

def migrate_normalized_structure(mysql_engine, mongo_client, batch_size=1000, dry_run=False):
    """Migrate using normalized structure (separate collections)"""
    
    log("\n" + "=" * 60)
    log("MIGRATING TO NORMALIZED MONGODB STRUCTURE")
    log("=" * 60)
    
    db = mongo_client[MONGO_DB]
    results = {
        'categories': {'copied': 0, 'errors': 0},
        'locations': {'copied': 0, 'errors': 0},
        'payment_methods': {'copied': 0, 'errors': 0},
        'customers': {'copied': 0, 'errors': 0},
        'items': {'copied': 0, 'errors': 0},
        'transactions': {'copied': 0, 'errors': 0}
    }
    
    # 1. Migrate Categories
    log("\n1. Migrating Categories...")
    try:
        with mysql_engine.connect() as conn:
            categories_df = pd.read_sql(text("SELECT * FROM Categories"), conn)
        
        if not dry_run:
            categories_docs = []
            for _, row in categories_df.iterrows():
                doc = {
                    '_id': row['CategoryID'],
                    'category_name': row['CategoryName'],
                    'created_at': row['CreatedAt'],
                    'updated_at': row['UpdatedAt']
                }
                categories_docs.append(doc)
            
            if categories_docs:
                db.categories.insert_many(categories_docs)
        
        results['categories']['copied'] = len(categories_df)
        log(f"  Categories: {len(categories_df)} documents {'would be ' if dry_run else ''}copied")
    
    except Exception as e:
        log(f"  ERROR migrating categories: {e}")
        results['categories']['errors'] = 1
    
    # 2. Migrate Locations
    log("\n2. Migrating Locations...")
    try:
        with mysql_engine.connect() as conn:
            locations_df = pd.read_sql(text("SELECT * FROM Locations"), conn)
        
        if not dry_run:
            locations_docs = []
            for _, row in locations_df.iterrows():
                doc = {
                    '_id': row['LocationID'],
                    'location_name': row['LocationName'],
                    'created_at': row['CreatedAt'],
                    'updated_at': row['UpdatedAt']
                }
                locations_docs.append(doc)
            
            if locations_docs:
                db.locations.insert_many(locations_docs)
        
        results['locations']['copied'] = len(locations_df)
        log(f"  Locations: {len(locations_df)} documents {'would be ' if dry_run else ''}copied")
    
    except Exception as e:
        log(f"  ERROR migrating locations: {e}")
        results['locations']['errors'] = 1
    
    # 3. Migrate Payment Methods
    log("\n3. Migrating Payment Methods...")
    try:
        with mysql_engine.connect() as conn:
            payment_methods_df = pd.read_sql(text("SELECT * FROM PaymentMethods"), conn)
        
        if not dry_run:
            payment_docs = []
            for _, row in payment_methods_df.iterrows():
                doc = {
                    '_id': row['PaymentMethodID'],
                    'payment_method_name': row['PaymentMethodName'],
                    'created_at': row['CreatedAt'],
                    'updated_at': row['UpdatedAt']
                }
                payment_docs.append(doc)
            
            if payment_docs:
                db.payment_methods.insert_many(payment_docs)
        
        results['payment_methods']['copied'] = len(payment_methods_df)
        log(f"  Payment Methods: {len(payment_methods_df)} documents {'would be ' if dry_run else ''}copied")
    
    except Exception as e:
        log(f"  ERROR migrating payment methods: {e}")
        results['payment_methods']['errors'] = 1
    
    # 4. Migrate Customers
    log("\n4. Migrating Customers...")
    try:
        with mysql_engine.connect() as conn:
            customers_df = pd.read_sql(text("SELECT * FROM Customers"), conn)
        
        if not dry_run:
            customers_docs = []
            for _, row in customers_df.iterrows():
                doc = {
                    '_id': row['CustomerID'],
                    'created_at': row['CreatedAt'],
                    'updated_at': row['UpdatedAt']
                }
                customers_docs.append(doc)
            
            if customers_docs:
                db.customers.insert_many(customers_docs)
        
        results['customers']['copied'] = len(customers_df)
        log(f"  Customers: {len(customers_df)} documents {'would be ' if dry_run else ''}copied")
    
    except Exception as e:
        log(f"  ERROR migrating customers: {e}")
        results['customers']['errors'] = 1
    
    # 5. Migrate Items
    log("\n5. Migrating Items...")
    try:
        with mysql_engine.connect() as conn:
            items_df = pd.read_sql(text("""
                SELECT i.ItemID, i.ItemName, i.PricePerUnit, i.CategoryID,
                       c.CategoryName, i.CreatedAt, i.UpdatedAt
                FROM Items i
                JOIN Categories c ON i.CategoryID = c.CategoryID
            """), conn)
        
        if not dry_run:
            items_docs = []
            for _, row in items_df.iterrows():
                doc = {
                    '_id': row['ItemID'],
                    'item_name': row['ItemName'],
                    'price_per_unit': float(row['PricePerUnit']),
                    'category_id': row['CategoryID'],
                    'category_name': row['CategoryName'],  # Denormalized for easier queries
                    'created_at': row['CreatedAt'],
                    'updated_at': row['UpdatedAt']
                }
                items_docs.append(doc)
            
            if items_docs:
                db.items.insert_many(items_docs)
        
        results['items']['copied'] = len(items_df)
        log(f"  Items: {len(items_df)} documents {'would be ' if dry_run else ''}copied")
    
    except Exception as e:
        log(f"  ERROR migrating items: {e}")
        results['items']['errors'] = 1
    
    # 6. Migrate Transactions (in batches)
    log("\n6. Migrating Transactions...")
    try:
        with mysql_engine.connect() as conn:
            # Get total count first
            total_transactions = conn.execute(text("SELECT COUNT(*) FROM Transactions")).scalar()
            log(f"  Total transactions to migrate: {total_transactions:,}")
            
            # Process in batches
            offset = 0
            total_copied = 0
            
            while offset < total_transactions:
                transactions_df = pd.read_sql(text(f"""
                    SELECT * FROM Transactions 
                    ORDER BY TransactionID 
                    LIMIT {batch_size} OFFSET {offset}
                """), conn)
                
                if len(transactions_df) == 0:
                    break
                
                if not dry_run:
                    transactions_docs = []
                    for _, row in transactions_df.iterrows():
                        doc = {
                            '_id': row['TransactionID'],
                            'customer_id': row['CustomerID'],
                            'item_id': row['ItemID'],
                            'payment_method_id': row['PaymentMethodID'],
                            'location_id': row['LocationID'],
                            'quantity': row['Quantity'],
                            'total_price': float(row['TotalPrice']),
                            'transaction_date': pd.to_datetime(row['TransactionDate']),
                            'discount_applied': bool(row['DiscountApplied']),
                            'created_at': row['CreatedAt'],
                            'updated_at': row['UpdatedAt']
                        }
                        transactions_docs.append(doc)
                    
                    if transactions_docs:
                        db.transactions.insert_many(transactions_docs)
                
                total_copied += len(transactions_df)
                offset += batch_size
                log(f"    Processed batch: {offset:,} / {total_transactions:,} transactions")
        
        results['transactions']['copied'] = total_copied
        log(f"  Transactions: {total_copied} documents {'would be ' if dry_run else ''}copied")
    
    except Exception as e:
        log(f"  ERROR migrating transactions: {e}")
        results['transactions']['errors'] = 1
    
    return results

def migrate_denormalized_structure(mysql_engine, mongo_client, batch_size=1000, dry_run=False):
    """Migrate using denormalized structure (embedded documents)"""
    
    log("\n" + "=" * 60)
    log("MIGRATING TO DENORMALIZED MONGODB STRUCTURE")
    log("=" * 60)
    
    db = mongo_client[MONGO_DB]
    results = {
        'customers': {'copied': 0, 'errors': 0},
        'transactions_with_details': {'copied': 0, 'errors': 0}
    }
    
    # 1. Migrate Customers (same as normalized)
    log("\n1. Migrating Customers...")
    try:
        with mysql_engine.connect() as conn:
            customers_df = pd.read_sql(text("SELECT * FROM Customers"), conn)
        
        if not dry_run:
            customers_docs = []
            for _, row in customers_df.iterrows():
                doc = {
                    '_id': row['CustomerID'],
                    'created_at': row['CreatedAt'],
                    'updated_at': row['UpdatedAt']
                }
                customers_docs.append(doc)
            
            if customers_docs:
                db.customers.insert_many(customers_docs)
        
        results['customers']['copied'] = len(customers_df)
        log(f"  Customers: {len(customers_df)} documents {'would be ' if dry_run else ''}copied")
    
    except Exception as e:
        log(f"  ERROR migrating customers: {e}")
        results['customers']['errors'] = 1
    
    # 2. Migrate Transactions with all details embedded
    log("\n2. Migrating Transactions with embedded details...")
    try:
        with mysql_engine.connect() as conn:
            # Get total count first
            total_transactions = conn.execute(text("SELECT COUNT(*) FROM Transactions")).scalar()
            log(f"  Total transactions to migrate: {total_transactions:,}")
            
            # Process in batches
            offset = 0
            total_copied = 0
            
            while offset < total_transactions:
                # Get transactions with all related data in one query
                transactions_df = pd.read_sql(text(f"""
                    SELECT 
                        t.TransactionID, t.CustomerID, t.Quantity, t.TotalPrice, 
                        t.TransactionDate, t.DiscountApplied, t.CreatedAt, t.UpdatedAt,
                        i.ItemID, i.ItemName, i.PricePerUnit,
                        c.CategoryID, c.CategoryName,
                        pm.PaymentMethodID, pm.PaymentMethodName,
                        l.LocationID, l.LocationName
                    FROM Transactions t
                    JOIN Items i ON t.ItemID = i.ItemID
                    JOIN Categories c ON i.CategoryID = c.CategoryID
                    JOIN PaymentMethods pm ON t.PaymentMethodID = pm.PaymentMethodID
                    JOIN Locations l ON t.LocationID = l.LocationID
                    ORDER BY t.TransactionID
                    LIMIT {batch_size} OFFSET {offset}
                """), conn)
                
                if len(transactions_df) == 0:
                    break
                
                if not dry_run:
                    transactions_docs = []
                    for _, row in transactions_df.iterrows():
                        doc = {
                            '_id': row['TransactionID'],
                            'customer_id': row['CustomerID'],
                            'quantity': row['Quantity'],
                            'total_price': float(row['TotalPrice']),
                            'transaction_date': pd.to_datetime(row['TransactionDate']),
                            'discount_applied': bool(row['DiscountApplied']),
                            'created_at': row['CreatedAt'],
                            'updated_at': row['UpdatedAt'],
                            # Embedded item details
                            'item': {
                                'item_id': row['ItemID'],
                                'item_name': row['ItemName'],
                                'price_per_unit': float(row['PricePerUnit'])
                            },
                            # Embedded category details
                            'category': {
                                'category_id': row['CategoryID'],
                                'category_name': row['CategoryName']
                            },
                            # Embedded payment method details
                            'payment_method': {
                                'payment_method_id': row['PaymentMethodID'],
                                'payment_method_name': row['PaymentMethodName']
                            },
                            # Embedded location details
                            'location': {
                                'location_id': row['LocationID'],
                                'location_name': row['LocationName']
                            }
                        }
                        transactions_docs.append(doc)
                    
                    if transactions_docs:
                        db.transactions_with_details.insert_many(transactions_docs)
                
                total_copied += len(transactions_df)
                offset += batch_size
                log(f"    Processed batch: {offset:,} / {total_transactions:,} transactions")
        
        results['transactions_with_details']['copied'] = total_copied
        log(f"  Transactions with details: {total_copied} documents {'would be ' if dry_run else ''}copied")
    
    except Exception as e:
        log(f"  ERROR migrating transactions with details: {e}")
        results['transactions_with_details']['errors'] = 1
    
    return results

def display_migration_summary(results, dry_run=False):
    """Display migration summary"""
    
    log("\n" + "=" * 60)
    if dry_run:
        log("DRY RUN SUMMARY - No data was actually copied")
    else:
        log("MIGRATION COMPLETED")
    log("=" * 60)
    
    total_copied = 0
    total_errors = 0
    
    for collection, stats in results.items():
        copied = stats['copied']
        errors = stats['errors']
        total_copied += copied
        total_errors += errors
        
        status = "[OK]" if errors == 0 else "[ERROR]"
        log(f"{status} {collection}: {copied:,} documents, {errors} errors")
    
    log("-" * 40)
    log(f"TOTAL: {total_copied:,} documents, {total_errors} errors")
    
    if total_errors > 0:
        log(f"WARNING: {total_errors} errors occurred during migration")
    
    if not dry_run and total_copied > 0:
        log(f">> Successfully copied {total_copied:,} documents to MongoDB!")

def main():
    """Main execution function"""
    
    parser = argparse.ArgumentParser(description='Migrate data from normalized MySQL to MongoDB')
    parser.add_argument('--batch-size', type=int, default=1000, help='Batch size for processing (default: 1000)')
    parser.add_argument('--denormalize', action='store_true', help='Use denormalized structure with embedded documents')
    parser.add_argument('--dry-run', action='store_true', help='Preview migration without copying data')
    
    args = parser.parse_args()
    
    # Clear previous log
    if LOG.exists():
        LOG.unlink()
    
    log("Starting MongoDB migration...")
    log(f"Source: MySQL database '{MYSQL_DB}'")
    log(f"Target: MongoDB database '{MONGO_DB}'")
    log(f"Structure: {'Denormalized' if args.denormalize else 'Normalized'}")
    log(f"Batch size: {args.batch_size}")
    
    if args.dry_run:
        log(">> DRY RUN MODE - No data will be actually copied")
    
    try:
        # Validate connections
        mysql_engine, mongo_client = validate_connections()
        
        # Get MySQL counts
        mysql_counts = get_mysql_counts(mysql_engine)
        total_records = sum(mysql_counts.values())
        
        if total_records == 0:
            log("No data found in MySQL database")
            return
        
        log(f"Total records to migrate: {total_records:,}")
        
        if not args.dry_run:
            # Clear existing MongoDB collections
            clear_mongodb_collections(mongo_client, args.denormalize)
        
        # Perform migration
        if args.denormalize:
            results = migrate_denormalized_structure(mysql_engine, mongo_client, args.batch_size, args.dry_run)
        else:
            results = migrate_normalized_structure(mysql_engine, mongo_client, args.batch_size, args.dry_run)
        
        # Display summary
        display_migration_summary(results, args.dry_run)
        
        log(f"\nFull log saved to: {LOG}")
        
    except Exception as e:
        log(f"ERROR during migration: {str(e)}")
        raise

if __name__ == "__main__":
    main()
