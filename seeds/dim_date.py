import pandas as pd
from datetime import datetime, timedelta

# Define start and end dates
start_date = datetime(2020, 1, 1)
end_date = datetime(2025, 12, 31)

# Generate list of dates
date_list = [start_date + timedelta(days=x) for x in range((end_date - start_date).days + 1)]

# Create DataFrame
dim_date_df = pd.DataFrame(date_list, columns=["date"])

# Add ISO week number and ISO year
dim_date_df["weeknum"] = dim_date_df["date"].apply(lambda x: x.isocalendar()[1])
dim_date_df["year"] = dim_date_df["date"].apply(lambda x: x.isocalendar()[0])

# Format date as YYYY-MM-DD
dim_date_df["date"] = dim_date_df["date"].dt.strftime("%Y-%m-%d")

# Display sample around year boundaries
print("Sample of dim_date around year boundaries:")
print(dim_date_df[dim_date_df["date"].between("2023-12-25", "2024-01-07")])
print(dim_date_df[dim_date_df["date"].between("2024-12-25", "2025-01-07")])
print(f"Total rows: {len(dim_date_df)}")

# Save to CSV
output_path = "dim_date.csv"
dim_date_df.to_csv(output_path, index=False)
print(f"dim_date saved to {output_path}")

# Save to Parquet
parquet_path = "dim_date.parquet"
dim_date_df.to_parquet(parquet_path, index=False)
print(f"dim_date also saved to {parquet_path}")