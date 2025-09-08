#!/usr/bin/env python3
"""Quick test of cleaning script"""

import pandas as pd
from pathlib import Path

try:
    # Paths
    ROOT = Path(__file__).resolve().parent
    RAW = ROOT / "dataset" / "retail_store_sales.csv"
    
    print("Loading dataset...")
    df = pd.read_csv(RAW)
    print(f"✓ Loaded: {df.shape[0]} rows, {df.shape[1]} columns")
    
    print("Checking missing values...")
    missing = df.isnull().sum()
    print(f"✓ Missing values found: {missing.sum()} total")
    
    print("Script test completed successfully!")
    
except Exception as e:
    print(f"ERROR: {e}")
    import traceback
    traceback.print_exc()
