import pandas as pd

# Load dataset
df = pd.read_csv("dataset/retail_store_sales.csv")

# Remove duplicates
df = df.drop_duplicates()

# Fill missing numeric values with median
for col in df.select_dtypes(include=['int64','float64']).columns:
    df[col].fillna(df[col].median(), inplace=True)

# Fill missing categorical values with mode
for col in df.select_dtypes(include=['object']).columns:
    df[col].fillna(df[col].mode()[0], inplace=True)

# Convert Transaction Date to datetime
df["Transaction Date"] = pd.to_datetime(df["Transaction Date"], errors="coerce")

# Save cleaned dataset
df.to_csv("dataset/retail_store_sales_clean.csv", index=False)
print("Data cleaned and saved to dataset/retail_store_sales_clean.csv")
