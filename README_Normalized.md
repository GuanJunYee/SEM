# Normalized Database Migration Pipeline

This project demonstrates a complete data migration pipeline from raw CSV data to normalized relational (MySQL) and document (MongoDB) databases, following industry best practices for data normalization and migration.

## Database Schema (Normalized)

Based on the provided ERD, the normalized database structure includes:

### **Reference Tables (Master Data)**
- **Categories**: Product categories with auto-increment IDs
- **Locations**: Store locations (Online, In-store, etc.)
- **PaymentMethods**: Payment types (Cash, Credit Card, Digital Wallet)
- **Customers**: Customer master data

### **Transactional Tables**
- **Items**: Products with pricing and category relationships
- **Transactions**: Main transaction table with foreign key relationships

### **Foreign Key Relationships**
```
Transactions → Customers (CustomerID)
Transactions → Items (ItemID)
Transactions → PaymentMethods (PaymentMethodID)
Transactions → Locations (LocationID)
Items → Categories (CategoryID)
```

## Migration Pipeline

### **Phase 1: Data Cleaning and Normalization**
```bash
python scripts/02_clean_dataset_normalized.py
```

**What it does:**
- Cleans raw CSV data (missing values, data types, business rules)
- Extracts unique values for reference tables
- Creates normalized CSV files for each table
- Validates data integrity and relationships
- Generates quality check reports

**Output Files:**
- `dataset/retail_store_sales_clean.csv` - Cleaned original data
- `dataset/categories.csv` - Categories reference table
- `dataset/locations.csv` - Locations reference table
- `dataset/payment_methods.csv` - Payment methods reference table
- `dataset/customers.csv` - Customers master data
- `dataset/items.csv` - Items with category relationships
- `dataset/transactions_normalized.csv` - Transactions with foreign key IDs

### **Phase 2: MySQL Normalized Database**
```bash
python scripts/03_migrate_to_mysql_normalized.py
```

**What it does:**
- Creates normalized MySQL database schema with proper foreign keys
- Loads reference tables first (respecting dependency order)
- Inserts transactional data with foreign key validation
- Creates indexes for optimal query performance
- Validates data integrity and business rules

**Features:**
- **Auto-increment primary keys** for reference tables
- **Foreign key constraints** ensure referential integrity
- **Optimized indexes** on frequently queried columns
- **Transaction safety** with rollback on errors
- **Comprehensive logging** and error reporting

### **Phase 3: MongoDB Migration (Dual Structure Support)**

#### **Option A: Normalized Structure**
```bash
python scripts/04_migrate_to_mongo_normalized.py --batch-size=1000
```

Creates separate MongoDB collections mirroring MySQL structure:
- `categories` - Category documents
- `locations` - Location documents  
- `payment_methods` - Payment method documents
- `customers` - Customer documents
- `items` - Item documents with category references
- `transactions` - Transaction documents with foreign key references

#### **Option B: Denormalized Structure**
```bash
python scripts/04_migrate_to_mongo_normalized.py --denormalize --batch-size=1000
```

Creates MongoDB-optimized embedded document structure:
- `customers` - Customer documents
- `transactions_with_details` - Transactions with embedded item, category, payment method, and location data

**Example Denormalized Document:**
```json
{
  "_id": "TXN_6867343",
  "customer_id": "CUST_09",
  "quantity": 10,
  "total_price": 185.0,
  "transaction_date": "2024-04-08",
  "discount_applied": true,
  "item": {
    "item_id": 15,
    "item_name": "Item_10_PAT",
    "price_per_unit": 18.5
  },
  "category": {
    "category_id": 3,
    "category_name": "Patisserie"
  },
  "payment_method": {
    "payment_method_id": 2,
    "payment_method_name": "Digital Wallet"
  },
  "location": {
    "location_id": 1,
    "location_name": "Online"
  }
}
```

#### **Migration Options:**
- `--batch-size=N` - Process N records at once (default: 1000)
- `--denormalize` - Use embedded document structure
- `--dry-run` - Preview migration without copying data

### **Phase 4: Migration Validation**
```bash
# Validate normalized structure
python scripts/05_validate_migration_normalized.py --structure=normalized

# Validate denormalized structure  
python scripts/05_validate_migration_normalized.py --structure=denormalized
```

**Validation Checks:**
- Record count verification across all tables/collections
- Data integrity validation (foreign key relationships)
- Business rule compliance (no negative quantities/prices)
- Cross-database consistency (MySQL vs MongoDB)
- Sample transaction spot-checks
- Business metrics comparison (revenue, quantities)
- Date range validation

## Configuration

### **Environment Setup**
Create a `.env` file in the project root:

```env
# MySQL Configuration
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASSWORD=your_password
MYSQL_DB=retail_db_normalized

# MongoDB Configuration
MONGO_URI=mongodb://localhost:27017
MONGO_DB=retail_store_normalized
```

### **Dependencies**
```bash
pip install -r requirement.txt
```

Required packages:
- `pandas` - Data manipulation and analysis
- `numpy` - Numerical computing
- `sqlalchemy` - SQL toolkit and ORM
- `pymysql` - MySQL database connector
- `pymongo` - MongoDB driver
- `python-dotenv` - Environment variable management

## Data Quality Features

### **Data Cleaning Operations**
1. **Missing Value Handling**
   - Transaction/Customer IDs: Remove rows (critical fields)
   - Categorical fields: Fill with "Unknown_FieldName"
   - Numeric fields: Fill with median values
   - Dates: Remove invalid dates

2. **Data Type Standardization**
   - Numeric fields: Convert to proper numeric types
   - Dates: Standardize to YYYY-MM-DD format
   - Booleans: Normalize various representations

3. **Business Rule Validation**
   - Remove negative quantities and prices
   - Validate Total Price = Quantity × Price Per Unit
   - Ensure referential integrity

### **Quality Metrics**
- **Completeness**: % of required fields populated
- **Accuracy**: Business rule compliance
- **Consistency**: Data format standardization  
- **Integrity**: Foreign key relationship validation
- **Uniqueness**: Primary key constraint enforcement

## Benefits of Normalization

### **Storage Efficiency**
- **Eliminates data redundancy** (category names stored once)
- **Reduces storage space** requirements
- **Maintains data consistency** across related records

### **Data Integrity**
- **Foreign key constraints** prevent orphaned records
- **Referential integrity** maintained automatically
- **Centralized master data** management

### **Query Performance**
- **Optimized indexes** on foreign key columns
- **Efficient joins** for complex queries
- **Scalable design** for large datasets

### **Maintenance Benefits**
- **Single source of truth** for reference data
- **Easy updates** to master data (e.g., category names)
- **Simplified data governance**

## Performance Considerations

### **Batch Processing**
- Configurable batch sizes for large datasets
- Memory-efficient processing with chunked reads
- Progress tracking with detailed logging

### **Database Optimization**
- **MySQL**: Foreign key indexes, query optimization
- **MongoDB**: Compound indexes, aggregation pipeline optimization
- **Connection pooling** for improved performance

### **Scalability Features**
- Support for millions of records
- Parallel processing capabilities
- Incremental migration support

## Monitoring and Logging

### **Comprehensive Logging**
All scripts generate detailed logs in the `results/` directory:
- Data cleaning operations and statistics
- Database schema creation and modifications
- Migration progress and error handling
- Validation results and data quality metrics

### **Quality Reports**
- CSV files with detailed quality metrics
- Data lineage and transformation tracking
- Error analysis and resolution recommendations

## 📚 Usage Examples

### **Complete Migration Workflow**
```bash
# 1. Clean and normalize data
python scripts/02_clean_dataset_normalized.py

# 2. Create normalized MySQL database
python scripts/03_migrate_to_mysql_normalized.py

# 3. Migrate to MongoDB (normalized)
python scripts/04_migrate_to_mongo_normalized.py

# 4. Validate migration
python scripts/05_validate_migration_normalized.py --structure=normalized

# Alternative: Denormalized MongoDB
python scripts/04_migrate_to_mongo_normalized.py --denormalize
python scripts/05_validate_migration_normalized.py --structure=denormalized
```

### **Development and Testing**
```bash
# Dry run migration (preview without copying)
python scripts/04_migrate_to_mongo_normalized.py --dry-run

# Process smaller batches for testing
python scripts/04_migrate_to_mongo_normalized.py --batch-size=100

# Validate specific structure type
python scripts/05_validate_migration_normalized.py --structure=denormalized
```

## 🚨 Troubleshooting

### **Common Issues**

1. **Missing CSV files**: Run `02_clean_dataset_normalized.py` first
2. **Database connection errors**: Check `.env` configuration
3. **Foreign key violations**: Verify data cleaning completed successfully
4. **Memory issues**: Reduce batch size for large datasets

### **Error Recovery**
- All operations are logged with timestamps
- Database operations use transactions for rollback capability
- Validation reports identify specific data integrity issues

## Project Structure

```
SEM/
├── scripts/
│   ├── 02_clean_dataset_normalized.py     # Data cleaning & normalization
│   ├── 03_migrate_to_mysql_normalized.py  # MySQL normalized migration
│   ├── 04_migrate_to_mongo_normalized.py  # MongoDB migration (dual structure)
│   └── 05_validate_migration_normalized.py # Comprehensive validation
├── dataset/
│   ├── retail_store_sales.csv             # Raw input data
│   └── [normalized CSV files]             # Generated reference tables
├── results/
│   └── [log files and reports]            # Migration logs and quality reports
├── .env                                    # Database configuration
└── requirement.txt                        # Python dependencies
```

This normalized approach follows enterprise-grade database design principles while maintaining compatibility with both relational and document database paradigms.
