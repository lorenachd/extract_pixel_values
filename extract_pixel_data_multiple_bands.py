# -*- coding: utf-8 -*-
"""
Created on Wed Aug 27 10:30:39 2025

@author: lorena carpes (aded the download of multiple bands in the original script)
"""

# extract pixel data from one or multiple bands:
    
#Ensure these packages are installed
import os
import requests
import json
import zipfile
import io
from osgeo import gdal
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from datetime import datetime
import numpy as np
from functools import reduce


######## add required info below

#Required input information
directory = r"C:\Users\marta\OneDrive - Hydrosat\lorena_one_drive\tasks_od\roula\0827_Weenat\data_band_3_soil_moist_root_zone_9_soil_wat_pot" #Change directory and new folder name to save data
os.makedirs(directory, exist_ok=True)

client_id = 'USR.P4fwj3qwfNUzPGhxejryEYWzDV7FZ5PT'  #Change API Key/Client ID (token) 
client_secret = 'hm9zSV9Y6FZsdR9gLFthZ7VrwHUpTBsuQYQNLJadHLAGqqffbM7VQMoyHZLwgGei' #Change API Password/Client Secret (token)

company_uuid = 'ab068aef-6798-44bb-a62c-4ba763081d45'  #Change Company UUID of interest
company_name = 'Weenat' #Change Company account name

order_list = [
    (36813, '45b364aa-b36b-4db6-96b8-de543b5f7d72'), #Change Order UUID of interest (1)
]

startdate = '2025-04-01' #Change Start date of period of interest
enddate = '2025-08-28' #Change End date of period of interest

band_number = [3,9] # add here one or more band numbers inside the brackets. must be a list. e.g. [3] (if only one band) or [1,2,3,4,5,6] (if multiple bands). DON'T REMOVE THE BRACKETS EVEN IF YOU WANT ONLY ONE BAND!!  

# add the coordinates of the locations you want the pixel values
points =[(48.789970, 4.171820), # plot 1 sensor
(50.40315, 1.83787), # plot 2 - W700329 30cm sensor
(50.40332, 1.83813), # plot 2 - W700A5A 30cm sensor
(50.403517, 1.83775), # plot 2 - 2082E28 30cm sensor
(50.40335, 1.837467), # plot 2 - 2089DDE 30cm sensor
(50.40335, 1.83745), # plot 2 - 201B83 60cm sensor
(50.40317, 1.8378), # plot 2 - W7007B5 60cm sensor 
(50.403367, -1.838117)] # plot 2 - 1B2659 60cm sensor

##########

#Building bridge with IrriWatch API - token (IGNORE)
def get_headers(client_id, client_secret):
    login_details = {'grant_type': 'client_credentials',
                     'client_id': client_id,
                     'client_secret': client_secret
                     }
    session = requests.Session()
    token_response = session.post(
        url='https://api.irriwatch.hydrosat.com/oauth/v2/token',
        data=login_details)
    token_dict = json.loads(token_response.text)
    bearer = token_dict['access_token']
    headers = {'Authorization': 'Bearer ' + bearer}
    return headers

#Building bridge with IrriWatch API - Company & Order (IGNORE)
def get_orders(company_uuid, headers):
    url_order_info = f"https://api.irriwatch.hydrosat.com/api/v1/company/{company_uuid}/order"
    field_data_response = requests.get(url=url_order_info, headers=headers)

    if field_data_response.status_code != 200:
        print(f"Failed to retrieve orders. Status code: {field_data_response.status_code}")
        return []

    try:
        token_dict = field_data_response.json()
        return token_dict
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        return []

#Building bridge with IrriWatch API - TIFF per day (Option to change band)
def download_tiffs(date, directory, company_uuid, order_number, order_uuid, headers):
    url_result_uuids = f"https://api.irriwatch.hydrosat.com/api/v1/company/{company_uuid}/order/{order_uuid}/result"
    result_uuids_response = requests.get(url=url_result_uuids, headers=headers)
    # band_number = band_number

    if result_uuids_response.status_code != 200:
        print(f"Failed to retrieve results for order {order_uuid}. Status code: {result_uuids_response.status_code}")
        return

    list_result_uuids = result_uuids_response.json()

    for result in list_result_uuids:
        result_date = result['date']
        result_uuid = result['uuid']

        if result_date == date.replace('-', ''):
            path_to_save = os.path.join(directory, str(order_number), result_date)
            os.makedirs(path_to_save, exist_ok=True)

            url_tiff_data = f"https://api.irriwatch.hydrosat.com/api/v1/company/{company_uuid}/order/{order_uuid}/result/{result_uuid}"
            tiff_data_response = requests.get(url=url_tiff_data, headers=headers)

            zipfile_data = zipfile.ZipFile(io.BytesIO(tiff_data_response.content))
            temp_extract_path = os.path.join(path_to_save, "temp")
            os.makedirs(temp_extract_path, exist_ok=True)

            zipfile_data.extractall(temp_extract_path)

             # it was single band value, an integer, but now its a list with more than one band number: https://stackoverflow.com/questions/60504217/typeerror-not-supported-between-instances-of-list-and-int
            for file in os.listdir(temp_extract_path):
                for band_nb in band_number:
                    if file.endswith(".tif"):
                        input_tiff_path = os.path.join(temp_extract_path, file)
                        output_tiff_path = os.path.join(path_to_save, f"{file[:-4]}_band{band_nb}.tif")
    
                     
                        dataset = gdal.Open(input_tiff_path)
                        if dataset is not None and dataset.RasterCount >= band_nb: #Provide here preferred band
                            selected_band = dataset.GetRasterBand(band_nb).ReadAsArray() #Provide here preferred band
                            driver = gdal.GetDriverByName("GTiff")
                            out_dataset = driver.Create(output_tiff_path, dataset.RasterXSize, dataset.RasterYSize, 1,
                                                        gdal.GDT_Float32)
    
                            # Copy georeferencing
                            out_dataset.SetGeoTransform(dataset.GetGeoTransform())
                            projection = dataset.GetProjection()
                            if not projection:
                                projection = 'EPSG:4326'
                            out_dataset.SetProjection(projection)
                            out_dataset.GetRasterBand(1).WriteArray(selected_band)
                            out_dataset.FlushCache()
                            out_dataset = None
                            dataset = None

            # Clean temp
            for file in os.listdir(temp_extract_path):
                os.remove(os.path.join(temp_extract_path, file))
            os.rmdir(temp_extract_path)

    return

#Create shapefile of given coordinates (IGNORE)
def create_point_shapefile(points, directory):

    if not points:
        print("No points provided. Skipping shapefile creation.")
        return

    shapefile_path = os.path.join(directory, "input_points.shp")

    # Remove existing shapefile if it exists
    if os.path.exists(shapefile_path):
        os.remove(shapefile_path)

    gdf = gpd.GeoDataFrame(
        {'Latitude': [p[0] for p in points], 'Longitude': [p[1] for p in points]},
        geometry=[Point(p[1], p[0]) for p in points],
        crs="EPSG:4326"
    )
    gdf.to_file(shapefile_path, driver='ESRI Shapefile')
    print(f"Shapefile created with {len(points)} points: {shapefile_path}")


#Extract pixel values of coordinates (Option to change TIFF name band)
def extract_selected_band_values(points, directory, output_csv):
    all_results = []

    for band_nb in band_number:
        band_results = []

        for root, dirs, files in os.walk(directory):
            for file in files:
                if file.endswith(f"_band{band_nb}.tif"): # here preferred band
                    tiff_path = os.path.join(root, file)
                    dataset = gdal.Open(tiff_path)
                    if dataset is None:
                        continue
                    
                    # Extract date from folder name
                    date_folder = os.path.basename(os.path.dirname(tiff_path))
                    if not date_folder.isdigit():
                        continue
                    
                    # Convert '20250316' → '2025-03-16'
                    formatted_date = datetime.strptime(date_folder, '%Y%m%d').strftime('%Y-%m-%d')

                    gt = dataset.GetGeoTransform()
                    band = dataset.GetRasterBand(1)
                    band_data = band.ReadAsArray()
                    nodata = band.GetNoDataValue()

                    for lat, lon in points:
                        # Convert geo coords to pixel coords
                        px = int((lon - gt[0]) / gt[1])
                        py = int((lat - gt[3]) / gt[5])

                        if (0 <= px < dataset.RasterXSize) and (0 <= py < dataset.RasterYSize):
                            value = band_data[py, px]
                            if nodata is not None and value == nodata:
                                continue
                            if np.isnan(value):
                                continue

                            band_results.append({
                                'Latitude': lat,
                                'Longitude': lon,
                                'Date': formatted_date,
                                f'Band{band_nb}_Value': value # band column name
                            })

                    dataset = None

        # Convert to df and append to all_results
        df_band = pd.DataFrame(band_results)
        all_results.append(df_band)
        
    #https://stackoverflow.com/questions/44327999/how-to-merge-multiple-dataframes    
    # Merge all band dfs
    df_merged = reduce(lambda left, right: pd.merge(left, right, on=['Date', 'Latitude', 'Longitude'], how='outer'), all_results)

    # Save to CSV
    df_merged.to_csv(output_csv, index=False)
    print(f"All band values extracted and saved to {output_csv}")

# ------------------ Main Execution ------------------------

headers = get_headers(client_id, client_secret)
orders = get_orders(company_uuid, headers)
dates = pd.date_range(startdate, enddate, freq="D")

create_point_shapefile(points, directory)

for order_number, order_uuid in order_list:
    for date in dates:
        formatted_date = date.strftime("%Y-%m-%d")
        print(f"Downloading data for Order {order_number} on {formatted_date}...")
        download_tiffs(formatted_date, directory, company_uuid, order_number, order_uuid, headers)

print("Download complete.")

band_str = "_".join(map(str, band_number))
output_csv = os.path.join(directory, f"band_{band_str}_values.csv")
extract_selected_band_values(points, directory, output_csv)

# output_csv = os.path.join(directory, f"band{band_number}_values_test_merge.csv") #(optional) Change CSV output name
# extract_selected_band_values(points, directory, output_csv)