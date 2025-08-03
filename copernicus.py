import cdsapi

# Initialize the API client
client = cdsapi.Client()

dataset = "cams-europe-air-quality-reanalyses"
request = {
    "variable": [
        "nitrogen_dioxide",
        "ozone",
        "particulate_matter_2.5um",
        "particulate_matter_10um",
        "sulphur_dioxide"
    ],
    "model": ["ensemble"],
    "level": ["0"],
    "type": ["interim_reanalysis"],
    "year": ["2024"],
    "month": ["06"],
}

client.retrieve(dataset, request).download("copernicus_data.nc")

print("Download complete: copernicus_data.nc")
