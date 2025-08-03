import os
import pandas as pd
import glob

# Folder where your CSV files are located
csv_folder = r"C:\Users\User\Documents\DIPLWMATIKH\Thesis_back"

# Pattern to match CSV files
csv_files = glob.glob(os.path.join(csv_folder, "*.csv"))

# List to hold data from each CSV
all_data = []

for file in csv_files:
    try:
        print(f"📂 Merging {file}...")
        # Read CSV and append to the list
        df = pd.read_csv(file, parse_dates=["Date"])
        all_data.append(df)
    except Exception as e:
        print(f"❌ Error reading {file}: {e}")

# Concatenate all dataframes and drop duplicates
merged_df = pd.concat(all_data, ignore_index=True).drop_duplicates()

# Format the 'Date' column:
# If pollutant is ozone → keep full datetime
# Else → strip time, keep only date
merged_df["Date"] = merged_df.apply(
    lambda row: row["Date"].date() if "Ozone" not in row["Pollutant"] else row["Date"],
    axis=1
)

# Save the merged dataframe to a new CSV file
merged_csv = "merged_2022.csv"
merged_df.to_csv(merged_csv, index=False)

print(f"✅ Merged CSV saved as {merged_csv}")
