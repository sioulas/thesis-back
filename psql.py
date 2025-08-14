from sqlalchemy import create_engine
import pandas as pd

df = pd.read_csv("merged_2024.csv")

# Rename CSV columns to match DB schema
df.rename(columns={
    "Region": "region",
    "Date": "date",
    "Pollutant": "pollutant",
    "Concentration_µg/m³": "concentration"
}, inplace=True)

df["geom"] = None  # Or pd.NA if preferred

# Correct DB connection string
engine = create_engine("postgresql+psycopg2://postgres:67206720Ks%40%40@127.0.0.1:5432/datacube")

# Insert into DB
df.to_sql("odc_data", engine, if_exists="append", index=False)


print("✅ 8-hour ozone data inserted without geom.")