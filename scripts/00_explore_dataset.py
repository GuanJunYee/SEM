"""
00_explore_dataset.py
Explores the raw dataset and writes findings to results/.
"""

import pandas as pd
from pathlib import Path

# paths
ROOT = Path(__file__).resolve().parents[1]          # .../SEM
DATA = ROOT / "dataset" / "retail_store_sales.csv"
RESULTS = ROOT / "results"
RESULTS.mkdir(exist_ok=True)

# load
df = pd.read_csv(DATA)

# ---------- console preview ----------
print("Shape:", df.shape)
print("\nInfo:")
print(df.info())
print("\nMissing Values per Column:")
print(df.isnull().sum())
print("\nNumber of duplicate rows:", df.duplicated().sum())
for col in ["Payment Method", "Location", "Discount Applied"]:
    print(f"\nUnique values in {col}:", df[col].unique())
print("\nSummary Statistics:")
print(df.describe())

# ---------- files to results/ ----------
# 1) full info + missing summary as a text report
report_path = RESULTS / "00_explore_report.txt"
with open(report_path, "w", encoding="utf-8") as f:
    f.write("== Dataset Overview ==\n")
    f.write(f"Shape: {df.shape}\n\n")

    f.write("== dtypes & non-null counts ==\n")
    df.info(buf=f)
    f.write("\n")

    f.write("== Missing values per column ==\n")
    f.write(df.isnull().sum().to_string())
    f.write("\n\n")

    f.write(f"== Duplicate rows ==\n{df.duplicated().sum()}\n\n")

    f.write("== Unique values (selected categoricals) ==\n")
    for col in ["Payment Method", "Location", "Discount Applied"]:
        f.write(f"{col}: {list(pd.Series(df[col].unique()).astype(str))}\n")
    f.write("\n== Summary statistics (numeric) ==\n")
    f.write(df.describe().to_string())
print(f"[saved] {report_path}")

# 2) machine-friendly CSVs
(df.isnull().sum()
   .rename("missing_count")
   .to_frame()
   .to_csv(RESULTS / "00_missing_counts.csv"))
df.describe().to_csv(RESULTS / "00_numeric_summary.csv")
print(f"[saved] {RESULTS / '00_missing_counts.csv'}")
print(f"[saved] {RESULTS / '00_numeric_summary.csv'}")

