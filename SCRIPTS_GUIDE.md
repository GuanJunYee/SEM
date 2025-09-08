# SEM Data Pipeline Scripts Guide

## Essential Scripts (Run in Order!)

### **`00_data_exploration.py`** - Dataset Analysis 🔍
**STEP 0 (Optional): Analyze raw data before processing**
```bash
python scripts\00_data_exploration.py
```

### **`01_data_pipeline_main.py`** - Main SEM Demo Pipeline ⭐
**STEP 1: Start here! Complete data processing pipeline**
```bash
# Full pipeline: Clean + Normalize + MySQL ready
python scripts\01_data_pipeline_main.py

# Add MongoDB denormalization 
python scripts\01_data_pipeline_main.py --denormalize-mongo

# Skip cleaning (use existing clean data)
python scripts\01_data_pipeline_main.py --skip-cleaning
```

## Database Migration

### **`02_mysql_migration_normalized.py`** - MySQL Import
**STEP 2: Load normalized tables into MySQL database**
```bash
python scripts\02_mysql_migration_normalized.py
```

### **`03_mongodb_migration_normalized.py`** - MongoDB Import
**STEP 3: Load data into MongoDB with optional denormalization**
```bash
python scripts\03_mongodb_migration_normalized.py
python scripts\03_mongodb_migration_normalized.py --denormalized
```

## Quality Assurance

### **`04_data_validation_normalized.py`** - Data Validation
**STEP 4: Validate data integrity across MySQL and MongoDB**
```bash
python scripts\04_data_validation_normalized.py
```

## Utility

### **`data_exploration.py`** - Dataset Analysis
**Optional: Explore and analyze raw dataset**
```bash
python scripts\data_exploration.py
```

## Complete Workflow (Copy & Paste!)

```bash
cd "c:\Users\user\Desktop\New folder\SEM"

# Step 1: Process data
python scripts\01_data_pipeline_main.py

# Step 2: Load to MySQL
python scripts\02_mysql_migration_normalized.py

# Step 3: Load to MongoDB  
python scripts\03_mongodb_migration_normalized.py

# Step 4: Validate everything
python scripts\04_data_validation_normalized.py
```

## What Each Script Creates

- **`01_`** → Clean data + normalized tables in `dataset/`
- **`02_`** → MySQL database with normalized schema
- **`03_`** → MongoDB collections (normalized or flat)
- **`04_`** → Validation reports in `results/`
