# -*- coding: utf-8 -*-
"""
Created on Thu Aug 28 13:20:26 2025

@author: lorena


"""


import pandas as pd

###### add required info below

file_name = "band_3_9_values.csv" # add your file name

file_path = r"C:\Users\marta\OneDrive - Hydrosat\lorena_one_drive\tasks_od\roula\0827_Weenat\data_band_3_soil_moist_root_zone_9_soil_wat_pot"  # add path to your file   



sensor_location = {
    "plot_1": [48.78997, 4.17182], # I had to delete the last 0 from the coordinates in the "Use cae metadata.txt" file: 48.789970, 4.171820
    "plot 2 - W700329 30cm": [50.40315,1.83787],
    "plot 2 - W700A5A 30cm": [50.40332,1.83813],
    "plot 2 - 2082E28 30cm": [50.403517,1.83775],
    "plot 2 - 2089DDE 30cm": [50.40335,1.837467],
    "plot 2 - 201B83 60cm": [50.40335,1.83745],
    "plot 2 - W7007B5 60cm": [50.40317,1.8378],
    "plot 2 - 1B2659 60cm": [50.403367,1.838117]
}


######

sensor_band_df = pd.read_csv(f"{file_path}\\{file_name}")

sensor_band_df["sensor_name"] = ""

# Assign sensor names based on latitude and longitude
for name, (lat, lon) in sensor_location.items():
    mask = (sensor_band_df["Latitude"] == lat) & (sensor_band_df["Longitude"] == lon)
    sensor_band_df.loc[mask, "sensor_name"] = name


band_columns = [col for col in sensor_band_df.columns if col.startswith("Band")] 

# Reorder columns
final_columns = ["Date", "sensor_name", "Latitude", "Longitude"] + band_columns
sensor_band_df = sensor_band_df[final_columns]

sensor_band_df.to_csv(f"{file_path}\\{file_name}", index=False)



