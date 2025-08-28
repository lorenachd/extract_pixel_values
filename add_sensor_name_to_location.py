# -*- coding: utf-8 -*-
"""
Created on Thu Aug 28 13:20:26 2025

@author: lorena



"""

import pandas as pd

file_path = r"C:\Users\marta\OneDrive - Hydrosat\lorena_one_drive\tasks_od\roula\0827_Weenat\data_band_10_moisture_status_test_new_code_order\band10_values.csv" # path to your file   
new_file_name = "band9_values_sensor.csv"  #  name of the new file, with the sensor_name column added
new_file_path = r"C:\Users\marta\OneDrive - Hydrosat\lorena_one_drive\tasks_od\roula\0827_Weenat\data_band_9_soil_water_potential" # path to the folder you want to save the file with the sensor_name column added
band_number = 10
# band_col_name = 'Band9_Value' # name of the column in the csv file with the band values


sensor_band_df = pd.read_csv(file_path)



sensor_location = {
    "plot_1": [48.78997, 4.17182], # I had to delete the last 0 from the coordinates: 48.789970, 4.171820
    "plot 2 - W700329 30cm": [50.40315,1.83787],
    "plot 2 - W700A5A 30cm": [50.40332,1.83813],
    "plot 2 - 2082E28 30cm": [50.403517,1.83775],
    "plot 2 - 2089DDE 30cm": [50.40335,1.837467],
    "plot 2 - 201B83 60cm": [50.40335,1.83745],
    "plot 2 - W7007B5 60cm": [50.40317,1.8378],
    "plot 2 - 1B2659 60cm": [50.403367,1.838117]
}



sensor_band_df["sensor_name"] = ""


sensor_band_df = sensor_band_df[['sensor_name','Latitude', 'Longitude', 'Date', f"Band{band_number}_Value"]]



for name, (lat, lon) in sensor_location.items():
    mask = (sensor_band_df["Latitude"] == lat) & (sensor_band_df["Longitude"] == lon)
    sensor_band_df.loc[mask, "sensor_name"] = name
    
    
sensor_band_df.to_csv(file_path, index=False) # in case you want to override the input file with the new one

# sensor_band_df.to_csv(f"{new_file_path}\\{new_file_name}", index=False)


