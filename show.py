import os
import xarray
import pandas as pd
import glob

# Get a list of all NetCDF files in the directory
nc_files = glob.glob("*.nc") 

pollutants = {
    "no2": "Nitrogen Dioxide (NO₂) µg/m³",
    "o3": "Ozone (O₃) µg/m³",  
    "pm2p5": "Particulate Matter 2.5 µg/m³",  
    "pm10": "Particulate Matter 10 µg/m³",  
    "so2": "Sulphur Dioxide (SO₂) µg/m³",  
}


# Define bounding boxes
cities = {
    "Mesolongi": (38.3735, 21.4310),
    "Nafplio": (37.5676, 22.8070),
    "Tripoli": (37.5089, 22.3790),
    "Arta": (39.1600, 20.9856),
    "Athens": (37.9838, 23.7275),
    "Patras": (38.2466, 21.7346),
    "Livadia": (38.4356, 22.8733),
    "Grevena": (40.0836, 21.4274),
    "Drama": (41.1528, 24.1473),
    "Rhodes": (36.4349, 28.2235),
    "Alexandroupoli": (40.8485, 25.8744),
    "Chalkida": (38.4630, 23.6020),
    "Karpenisi": (38.9122, 21.7933),
    "Zakynthos": (37.7870, 20.8990),
    "Pyrgos": (37.6751, 21.4417),
    "Veria": (40.5247, 22.2029),
    "Heraklion": (35.3387, 25.1442),
    "Igoumenitsa": (39.5038, 20.2653),
    "Thessaloniki": (40.6401, 22.9444),
    "Ioannina": (39.6675, 20.8503),
    "Kavala": (40.9396, 24.4069),
    "Karditsa": (39.3640, 21.9217),
    "Kastoria": (40.5177, 21.2687),
    "Corfu": (39.6243, 19.9217),
    "Argostoli": (38.1760, 20.4890),
    "Kilkis": (40.9939, 22.8800),
    "Kozani": (40.3006, 21.7885),
    "Corinth": (37.9402, 22.9510),
    "Ermoupoli": (37.4448, 24.9410),
    "Sparta": (37.0735, 22.4297),
    "Larissa": (39.6413, 22.4133),
    "Agios Nikolaos": (35.1910, 25.7170),
    "Mytilene": (39.1106, 26.5547),
    "Lefkada": (38.8300, 20.7000),
    "Volos": (39.3620, 22.9425),
    "Kalamata": (37.0391, 22.1126),
    "Xanthi": (41.1415, 24.8836),
    "Edessa": (40.8025, 22.0470),
    "Katerini": (40.2719, 22.5026),
    "Preveza": (38.9562, 20.7505),
    "Rethymno": (35.3667, 24.4833),
    "Komotini": (41.1191, 25.4054),
    "Samos": (37.7548, 26.9770),
    "Serres": (41.0850, 23.5490),
    "Trikala": (39.5559, 21.7670),
    "Lamia": (38.9000, 22.4333),
    "Florina": (40.7810, 21.4090),
    "Amfissa": (38.5250, 22.3800),
    "Chania": (35.5138, 24.0180),
    "Kos": (36.8938, 27.2877),
    "Polygyros": (40.3814, 23.4425),
    "Fira": (36.4165, 25.4335),
    "Moudros": (39.8885, 25.2437),
}

# CSV filename
csv_file = "cities_air_quality_daily.csv"

# Load existing data if the CSV file already exists
if os.path.exists(csv_file):
    existing_df = pd.read_csv(csv_file)
else:
    existing_df = pd.DataFrame(columns=["Region", "Date", "Pollutant", "Concentration_µg/m³"])

# Prepare a DataFrame to store all results
all_results = []

# Process each NetCDF file
# Process each NetCDF file
for nc_file in nc_files:
    print(f"📂 Processing {nc_file}...")

    try:
        # Open the dataset
        data = xarray.open_dataset(nc_file, engine="netcdf4")

        # Detect the pollutant in the file
        detected_pollutant = None
        for key in pollutants:
            if key in data.variables:
                detected_pollutant = key
                break

        if detected_pollutant is None:
            print(f"⚠️ Skipping {nc_file}: No known pollutant found.")
            continue

        for region, (lat, lon) in cities.items():
            # Select data for the nearest location
            region_data = data[detected_pollutant].sel(lat=lat, lon=lon, method="nearest")

            # Decide resampling frequency
            pollutant_label = pollutants[detected_pollutant]
            if "Ozone" in pollutant_label:
                # 8-hour average for Ozone
                averaged_data = region_data.resample(time="8h").mean()
            else:
                # Daily average for other pollutants
                averaged_data = region_data.resample(time="1D").mean()

            # Store results
            for time, value in zip(averaged_data.time.values, averaged_data.values):
                all_results.append([
                    region,
                    pd.to_datetime(time),
                    pollutant_label,
                    value
                ])

    except Exception as e:
        print(f"❌ Error processing {nc_file}: {e}")

# Convert to DataFrame
df = pd.DataFrame(all_results, columns=["Region", "Date", "Pollutant", "Concentration_µg/m³"])

# Save to CSV
df.to_csv("cities_air_quality_daily_07.csv", index=False)

print("✅ All data saved as cities_air_quality_daily_01.csv")
