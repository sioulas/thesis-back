# import pandas as pd
# from sqlalchemy import create_engine

# # Load your merged CSV
# csv_file = "merged_air_quality.csv"
# df = pd.read_csv(csv_file)

# # Make sure 'Date' column is parsed as datetime
# df['Date'] = pd.to_datetime(df['Date'], errors='coerce')

# # Create connection to PostgreSQL using SQLAlchemy
# engine = create_engine('postgresql://postgres:yourPassword@localhost:5432/datacube')


# # Write the DataFrame to a new table, replace 'air_quality' with your desired table name
# df.to_sql('air_quality_2023', engine, if_exists='replace', index=False)

# print("✅ CSV data inserted into PostgreSQL!")

import pandas as pd
from sqlalchemy import create_engine

# Database connection URL
engine = create_engine("postgresql+psycopg2://postgres:67206720Ks%40%40@127.0.0.1:5432/datacube")

# Load CSV
df = pd.read_csv("merged_air_quality.csv")

# Optional: convert date column to just the date
df['date'] = pd.to_datetime(df['date']).dt.date

# Insert into DB
df.to_sql("air_quality_2023", engine, if_exists="replace", index=False)

print("✅ Data inserted successfully!")
