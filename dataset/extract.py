import zipfile

with zipfile.ZipFile("dataset/retail-store-sales-dirty-for-data-cleaning.zip", "r") as zip_ref:
    zip_ref.extractall("dataset")

# Load CSV
import pandas as pd
df = pd.read_csv("dataset/retail_store_sales.csv")
print(df.head())
