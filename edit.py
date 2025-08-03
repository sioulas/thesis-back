import os
import pandas as pd
import glob

# Folder where your CSV files are located
csv_folder = r"C:\Users\User\OneDrive\Documents\ODC_Data\Thessaloniki"  # Update if needed

# Find all CSV files in the folder
csv_files = glob.glob(os.path.join(csv_folder, "*.csv"))

for file in csv_files:
    print(f"🧹 Cleaning {file}...")
    try:
        df = pd.read_csv(file)

        # Drop the "File" column if it exists
        if "File" in df.columns:
            df.drop(columns=["File"], inplace=True)

            # Save the cleaned DataFrame back to the same file
            df.to_csv(file, index=False)
            print(f"✅ Removed 'File' column from {os.path.basename(file)}")
        else:
            print(f"ℹ️ 'File' column not found in {os.path.basename(file)} — skipping.")

    except Exception as e:
        print(f"❌ Error processing {file}: {e}")
