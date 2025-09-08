# SEM Data Migration Pipeline

A comprehensive, enterprise-grade data migration pipeline that demonstrates efficient data transformation from raw CSV to normalized relational (MySQL) and document-based (MongoDB) databases with advanced data quality assurance.

## 🎯 Overview

This project implements a production-ready data migration pipeline with smart data recovery, comprehensive validation, and dual database architecture. The pipeline processes retail sales data through multiple stages while maintaining data integrity and providing detailed audit trails.

### Key Achievements
- **95.2% data retention** through intelligent recovery algorithms
- **Enterprise-grade reliability** with idempotent operations
- **Dual database architecture** (MySQL + MongoDB) with perfect synchronization
- **Comprehensive validation** with 15+ quality assurance categories
- **Production-ready features** including audit logging and batch reconciliation

## 🏗️ Architecture

```
Raw CSV Data (12,575 rows)
    ↓
Smart Data Cleaning & Recovery (95.2% retention)
    ↓
Normalized CSV Tables (organized structure)
    ↓
MySQL Database (relational model)
    ↓
MongoDB Database (document model)
    ↓
Comprehensive Validation (15+ categories)
```

## 📁 Project Structure

```
SEM/
├── dataset/
│   ├── retail_store_sales.csv              # Raw data input
│   ├── retail_store_sales_clean.csv        # Cleaned data output
│   └── normalized/                          # Organized normalized tables
│       ├── categories.csv
│       ├── customers.csv
│       ├── items.csv
│       ├── locations.csv
│       ├── payment_methods.csv
│       └── transactions_normalized.csv
│
├── scripts/
│   ├── 00_data_exploration.py              # Initial data analysis
│   ├── 01_data_pipeline_main.py            # Enhanced data cleaning
│   ├── 02_mysql_migration_normalized.py    # MySQL migration
│   ├── 03_mongodb_migration_normalized.py  # MongoDB migration
│   └── 04_data_validation_normalized.py    # Comprehensive validation
│
├── results/                                 # Generated reports & logs
│   ├── 01_explore_report.txt
│   ├── 01_enhanced_pipeline.log
│   ├── 03_mysql_normalized_schema.sql
│   ├── 03_mysql_normalized_load_log.txt
│   ├── 04_mongo_normalized_load_log.txt
│   ├── 05_validation_report_normalized.txt
│   └── migration_audit.log
│
├── README.md                               # This documentation
├── IMPLEMENTED_FEATURES_SUMMARY.md        # Feature implementation details
└── NORMALIZED_FOLDER_ORGANIZATION.md      # File organization guide
```

## 🚀 Quick Start

### Prerequisites
```bash
# Python packages
pip install pandas numpy pymongo mysql-connector-python

# Database services
# MySQL Server (local or remote)
# MongoDB Server (local or remote)
```

### Database Setup
```sql
-- MySQL: Create database
CREATE DATABASE retail_db;
CREATE USER 'retail_user'@'localhost' IDENTIFIED BY 'password123';
GRANT ALL PRIVILEGES ON retail_db.* TO 'retail_user'@'localhost';
```

### Complete Pipeline Execution
```bash
# Navigate to project directory
cd "C:\Users\user\Desktop\New folder\SEM"

# Run complete pipeline in sequence
python scripts\00_data_exploration.py      # Data analysis
python scripts\01_data_pipeline_main.py    # Data cleaning & normalization
python scripts\02_mysql_migration_normalized.py    # MySQL migration
python scripts\03_mongodb_migration_normalized.py  # MongoDB migration
python scripts\04_data_validation_normalized.py    # Final validation
```

## 📋 Detailed Script Documentation

### 1. Data Exploration (`00_data_exploration.py`)

**Purpose**: Analyze raw dataset to understand structure, quality, and missing data patterns.

**Features**:
- Missing value analysis by column
- Data type detection and validation
- Statistical summaries for numeric columns
- Categorical value distribution analysis
- Duplicate detection

**Generated Files**:
- `results/01_explore_report.txt` - Comprehensive analysis report
- `results/01_missing_counts.csv` - Missing values breakdown
- `results/01_numeric_summary.csv` - Statistical summaries

**Key Insights**:
```
Dataset: 12,575 transactions, 11 columns
Missing Data:
- Item: 1,213 missing (9.6%)
- Price Per Unit: 609 missing (4.8%)
- Quantity: 604 missing (4.8%)
- Total Spent: 604 missing (4.8%)
- Discount Applied: 4,199 missing (33.4%)
```

### 2. Enhanced Data Pipeline (`01_data_pipeline_main.py`)

**Purpose**: Clean raw data and create normalized table structure with intelligent missing value recovery.

**Advanced Features**:
- **Smart Missing Value Recovery**: Uses mathematical relationships to recover data
- **Business Logic Validation**: Ensures Total = Price × Quantity consistency
- **Organized Output Structure**: Saves normalized tables to dedicated subfolder
- **Comprehensive Logging**: Detailed recovery and cleaning reports

**Recovery Algorithms**:
```python
# Price recovery from existing data
recovered_price = total_spent / quantity

# Item inference using category-price patterns
item_mapping = category_price_to_items[category, recovered_price]

# Mathematical validation
assert abs(total_spent - (recovered_price * quantity)) < 0.01
```

**Generated Files**:
- `dataset/retail_store_sales_clean.csv` - Cleaned dataset (11,971 rows)
- `dataset/normalized/` - 6 normalized CSV tables
- `results/01_enhanced_pipeline.log` - Detailed cleaning report
- `results/01_dropped_rows_report.csv` - Unrecoverable data analysis

**Results**:
- **95.2% data retention** (11,971 of 12,575 rows)
- **7,229 missing values recovered** through intelligent algorithms
- **Zero missing values** in final normalized tables

### 3. MySQL Migration (`02_mysql_migration_normalized.py`)

**Purpose**: Create normalized relational database with enterprise-grade reliability features.

**Enterprise Features**:
- **Idempotent Writes (UPSERT)**: Safe re-execution using `ON DUPLICATE KEY UPDATE`
- **Per-Batch Reconciliation**: Real-time data verification during loading
- **Enhanced Audit Logging**: Structured event logging with timestamps
- **Foreign Key Integrity**: Complete referential integrity enforcement
- **Business Rule Validation**: Comprehensive data quality checks

**Database Schema**:
```sql
Categories (8 records)
├── CategoryID (PK)
├── CategoryName
└── timestamps

Locations (2 records)
├── LocationID (PK)
├── LocationName
└── timestamps

PaymentMethods (3 records)
├── PaymentMethodID (PK)
├── PaymentMethodName
└── timestamps

Customers (25 records)
├── CustomerID (PK)
├── CustomerName
└── timestamps

Items (200 records)
├── ItemID (PK)
├── ItemName
├── CategoryID (FK)
├── Price
└── timestamps

Transactions (11,971 records)
├── TransactionID (PK)
├── CustomerID (FK)
├── ItemID (FK)
├── LocationID (FK)
├── PaymentMethodID (FK)
├── TransactionDate
├── Quantity
├── TotalPrice
├── DiscountApplied
└── timestamps
```

**Generated Files**:
- `results/03_mysql_normalized_schema.sql` - Complete DDL schema
- `results/03_mysql_normalized_load_log.txt` - Migration execution log
- `results/migration_audit.log` - Structured audit trail

### 4. MongoDB Migration (`03_mongodb_migration_normalized.py`)

**Purpose**: Migrate normalized data from MySQL to MongoDB while preserving relational structure.

**Features**:
- **Preserved Relational Structure**: Maintains foreign key relationships as document references
- **Batch Processing**: Efficient handling of large transaction datasets
- **Clean Slate Approach**: Drops existing collections for fresh migration
- **Error Handling**: Comprehensive error tracking and reporting
- **Connection Validation**: Verifies both MySQL and MongoDB connectivity

**MongoDB Collections**:
```javascript
// retail_store_normalized database
{
  "categories": 8 documents,
  "locations": 2 documents,
  "payment_methods": 3 documents,
  "customers": 25 documents,
  "items": 200 documents,
  "transactions": 11971 documents
}
// Total: 12,209 documents
```

**Document Structure Example**:
```javascript
// Transaction document
{
  "_id": ObjectId("..."),
  "TransactionID": 1001,
  "CustomerID": 15,
  "ItemID": 42,
  "LocationID": 1,
  "PaymentMethodID": 2,
  "TransactionDate": "2023-01-15",
  "Quantity": 3,
  "TotalPrice": 89.97,
  "DiscountApplied": true
}
```

**Generated Files**:
- `results/04_mongo_normalized_load_log.txt` - MongoDB migration log

### 5. Comprehensive Validation (`04_data_validation_normalized.py`)

**Purpose**: Perform extensive validation across all data sources to ensure migration integrity.

**15+ Validation Categories**:

#### **Core Integrity Checks**:
- ✅ **Schema Validation**: Table/collection structure verification
- ✅ **Record Count Comparison**: Exact count matching across databases
- ✅ **Foreign Key Integrity**: Zero orphaned records validation
- ✅ **Data Type Consistency**: Column type verification

#### **Business Logic Validation**:
- ✅ **Mathematical Consistency**: Total = Price × Quantity validation
- ✅ **Non-negative Constraints**: Positive quantities and prices
- ✅ **Date Range Validation**: Valid transaction dates
- ✅ **Categorical Value Verification**: Valid enum values

#### **Cross-Database Verification**:
- ✅ **Revenue Reconciliation**: Total revenue matching
- ✅ **Quantity Reconciliation**: Total quantities matching
- ✅ **Sample Record Verification**: Individual record comparison
- ✅ **Aggregation Consistency**: Summary statistics matching

#### **Advanced Quality Checks**:
- ✅ **Referential Integrity**: Complete relationship validation
- ✅ **Duplicate Detection**: Unique constraint verification
- ✅ **Data Completeness**: Missing value analysis
- ✅ **Migration Success Rate**: Overall pipeline efficiency

**Validation Results**:
```
✅ Perfect Record Alignment:
   MySQL: 12,209 total records
   MongoDB: 12,209 total records
   Match Rate: 100.00%

✅ Business Metrics Verification:
   Total Revenue: $1,552,071.00 (identical)
   Total Quantity: 66,276 items (perfect match)
   Migration Success Rate: 100.00%

✅ Data Integrity Assessment:
   Foreign Key Violations: 0 issues
   Business Rule Violations: 0 issues
   Migration Errors: 0 errors
```

**Generated Files**:
- `results/05_validation_report_normalized.txt` - Complete validation report

## 🎯 Key Features

### Data Quality & Recovery
- **Smart Missing Value Recovery**: Mathematical and pattern-based recovery algorithms
- **Business Rule Validation**: Ensures Total = Price × Quantity consistency
- **95.2% Data Retention**: Minimal data loss through intelligent recovery
- **Zero Missing Values**: Complete datasets after processing

### Enterprise Reliability
- **Idempotent Operations**: Safe script re-execution using UPSERT statements
- **Per-Batch Reconciliation**: Real-time data verification during migration
- **Enhanced Audit Logging**: Complete structured audit trails
- **Error Handling**: Comprehensive error tracking and reporting

### Database Architecture
- **Dual Database Support**: Synchronized MySQL and MongoDB databases
- **Normalized Structure**: Efficient relational data model
- **Foreign Key Integrity**: Complete referential integrity enforcement
- **Scalable Design**: Batch processing for large datasets

### Validation & Quality Assurance
- **15+ Validation Categories**: Comprehensive data quality checks
- **Cross-Database Verification**: Multi-system consistency validation
- **Business Logic Compliance**: Domain-specific rule enforcement
- **Migration Integrity**: End-to-end pipeline verification

### Organization & Maintainability
- **Organized File Structure**: Clean separation of normalized tables
- **Comprehensive Documentation**: Detailed logging and reporting
- **Modular Design**: Independent, reusable script components
- **Production-Ready**: Enterprise-grade reliability and monitoring

## 📊 Performance Metrics

| Metric | Value | Description |
|--------|-------|-------------|
| **Data Retention** | 95.2% | Percentage of original data preserved |
| **Recovery Success** | 7,229 values | Missing values intelligently recovered |
| **Migration Accuracy** | 100% | Perfect cross-database synchronization |
| **Data Integrity** | 0 violations | Zero foreign key or business rule violations |
| **Processing Efficiency** | 12,575 → 11,971 | Records processed with minimal loss |
| **Validation Coverage** | 15+ categories | Comprehensive quality assurance |

## 🔧 Configuration

### Database Connections
```python
# MySQL Configuration
MYSQL_CONFIG = {
    'host': 'localhost',
    'user': 'retail_user',
    'password': 'password123',
    'database': 'retail_db'
}

# MongoDB Configuration
MONGODB_CONFIG = {
    'host': 'localhost',
    'port': 27017,
    'database': 'retail_store_normalized'
}
```

### Processing Parameters
```python
# Batch processing settings
BATCH_SIZE = 1000           # Records per batch
DRY_RUN = False            # Preview mode
ENABLE_RECONCILIATION = True  # Per-batch verification
AUDIT_LOGGING = True       # Enhanced logging
```

## 🚀 Advanced Usage

### Individual Script Execution
```bash
# Data exploration only
python scripts\00_data_exploration.py

# Data cleaning only
python scripts\01_data_pipeline_main.py

# MySQL migration only
python scripts\02_mysql_migration_normalized.py

# MongoDB migration only
python scripts\03_mongodb_migration_normalized.py

# Validation only
python scripts\04_data_validation_normalized.py
```

### Dry Run Mode
```python
# Preview changes without executing
DRY_RUN = True  # Set in script configuration
```

### Custom Batch Sizes
```python
# Adjust for memory constraints or performance
BATCH_SIZE = 500   # Smaller batches for limited memory
BATCH_SIZE = 2000  # Larger batches for better performance
```

## 📈 Monitoring & Troubleshooting

### Log Files Location
```
results/
├── migration_audit.log              # Complete audit trail
├── 01_enhanced_pipeline.log         # Data cleaning details
├── 03_mysql_normalized_load_log.txt # MySQL migration log
├── 04_mongo_normalized_load_log.txt # MongoDB migration log
└── 05_validation_report_normalized.txt # Final validation
```

### Common Issues & Solutions

**Issue**: MySQL connection errors
```bash
# Solution: Verify MySQL service and credentials
mysql -u retail_user -p retail_db
```

**Issue**: MongoDB connection timeouts
```bash
# Solution: Check MongoDB service status
mongosh --host localhost:27017
```

**Issue**: High memory usage with large datasets
```python
# Solution: Reduce batch size
BATCH_SIZE = 500  # Reduce from default 1000
```

## 🏆 Production Readiness

This pipeline demonstrates enterprise-grade features suitable for production environments:

- ✅ **Data Quality Assurance**: Comprehensive validation and recovery
- ✅ **Reliability**: Idempotent operations and error handling
- ✅ **Monitoring**: Complete audit trails and logging
- ✅ **Scalability**: Batch processing and configurable parameters
- ✅ **Maintainability**: Modular design and comprehensive documentation
- ✅ **Compliance**: Data integrity and referential consistency


## 📝 License

This project is designed for educational and demonstration purposes, showcasing enterprise-grade data migration patterns and best practices.

## 🤝 Contributing

The pipeline demonstrates production-ready patterns that can be adapted for various data migration scenarios. Key patterns include smart data recovery, dual database architecture, and comprehensive validation frameworks.

---

**🎊 SEM Data Migration Pipeline - Enterprise-Grade Data Migration Made Simple**
