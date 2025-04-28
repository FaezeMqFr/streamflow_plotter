#!/usr/bin/env python3
"""
Streamflow Data Plotter for NWM and USGS Data

This script retrieves streamflow data from two sources:
 - NOAA's National Water Model (NWM) v2.1 and v3 (accessed via AWS using Zarr)
 - USGS data from the Water Services API

It then creates a plot comparing streamflow data from:
 - NWM v2.1
 - NWM v3
 - USGS

Usage:
    python streamflow_plotter.py
"""

# Import needed libraries
import xarray as xr
import numpy as np
import s3fs
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import requests
import pandas as pd
from datetime import datetime

# Conversion factor from cubic feet per second (cfs) to cubic meters per second (cms)
CFS_TO_CMS = 0.0283168

# USGS Parameter Code
parameter_code = '00060'  # Discharge in cfs; for some stations, tidal filter ID is 72137;00060 Discharge

# Function to retrieve USGS data from Water Services API and convert from cfs to cms
def ReadUSGSData(station_id, start_date, end_date):
    url = f"https://waterservices.usgs.gov/nwis/iv/?format=json&sites={station_id}&startDT={start_date}&endDT={end_date}&parameterCd={parameter_code}"
    response = requests.get(url)
    data = response.json()

    # Check if timeSeries data is available
    if not data['value']['timeSeries']:
        raise ValueError(
            f"No USGS data available for station {station_id} from {start_date} to {end_date}"
        )

    # Extract flow values and timestamps
    flow_vals = []
    time_vals = []
    for value in data['value']['timeSeries'][0]['values'][0]['value']:
        flow_vals.append(float(value['value']) * CFS_TO_CMS)  # Convert cfs to cms
        time_vals.append(pd.to_datetime(value['dateTime']))

    return time_vals, flow_vals

# Function to retrieve the time series for a specified reach ID
def GetTimeSeriesAtReach(store, reach_id, start_time_index, end_time_index):
    streamflow_array = store['streamflow']
    feature_id_array = store['feature_id']
    flows = streamflow_array.where(feature_id_array == reach_id, drop=True)
    return flows[start_time_index:end_time_index].values

# Function to get the time array directly without conversion
def GetTimeArray(store, start_time_index, end_time_index):
    time_array = store['time']
    return time_array[start_time_index:end_time_index].values

# Function to read NWM v2.1 data from AWS without downloading
def ReadNWMv21Data(reach_id, start_time, end_time):
    url = "s3://noaa-nwm-retrospective-2-1-zarr-pds/chrtout.zarr"
    fs = s3fs.S3FileSystem(anon=True)
    store = xr.open_zarr(s3fs.S3Map(url, s3=fs), consolidated=False)

    zero_start_time = np.datetime64('1979-02-01T00:00:00')
    start_time_index = int((np.datetime64(start_time) - zero_start_time) / np.timedelta64(1, 'h'))
    end_time_index = int((np.datetime64(end_time) - zero_start_time) / np.timedelta64(1, 'h'))

    time_series = GetTimeSeriesAtReach(store, reach_id, start_time_index, end_time_index)
    time_vals = GetTimeArray(store, start_time_index, end_time_index)

    return time_vals, time_series

# Function to read NWM v3 data from AWS without downloading
def ReadNWMv3Data(reach_id, start_time, end_time):
    url = "s3://noaa-nwm-retrospective-3-0-pds/CONUS/zarr/chrtout.zarr"
    fs = s3fs.S3FileSystem(anon=True)
    store = xr.open_zarr(s3fs.S3Map(url, s3=fs), consolidated=False)

    zero_start_time = np.datetime64('1979-02-01T00:00:00')
    start_time_index = int((np.datetime64(start_time) - zero_start_time) / np.timedelta64(1, 'h'))
    end_time_index = int((np.datetime64(end_time) - zero_start_time) / np.timedelta64(1, 'h'))

    time_series = GetTimeSeriesAtReach(store, reach_id, start_time_index, end_time_index)
    time_vals = GetTimeArray(store, start_time_index, end_time_index)

    return time_vals, time_series

# Function to truncate data for plotting (ensures v2.1 and v3 have the same length)
def TruncateForPlotting(time_vals_v21, flow_vals_v21, time_vals_v3, flow_vals_v3):
    min_length = min(len(flow_vals_v21), len(flow_vals_v3))
    return time_vals_v21[:min_length], flow_vals_v21[:min_length], time_vals_v3[:min_length], flow_vals_v3[:min_length]

# Function to create the streamflow graph with the x-axis formatted for daily tick intervals
def CreateStreamflowGraph(time_vals_v21, flow_vals_v21, time_vals_v3, flow_vals_v3, time_vals_usgs, flow_vals_usgs):
    plt.figure(figsize=(10, 6))

    # Plot NWM v2.1 data
    plt.plot(time_vals_v21, flow_vals_v21, label='NWM v2.1 Streamflow', color='blue')
    # Plot NWM v3 data
    plt.plot(time_vals_v3, flow_vals_v3, label='NWM v3 Streamflow', color='red')
    # Plot USGS data
    plt.plot(time_vals_usgs, flow_vals_usgs, label='USGS Streamflow',
             color='black', linestyle='--')

    # Format the x-axis to show daily timestamps
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.gca().xaxis.set_major_locator(mdates.DayLocator())  # Tick every day

    plt.xlabel('Date (Daily)')
    plt.ylabel('Streamflow (cms)')
    plt.title(f'Daily Streamflow at Fish River Near Silver Hill, AL (NWMID#{reach_id})')
    plt.legend(loc='upper right')

    plt.xticks(rotation=45)
    plt.grid(True, linestyle='--')
    plt.tight_layout()
    plt.show()

# Main execution
if __name__ == "__main__":
    reach_id = 18514402  # NWM reach ID
    usgs_station_id = "02378500"  # USGS station ID
    start_time = '2020-08-24T00:00:00'
    end_time = '2020-09-03T23:59:59'

    # Retrieve NWM v2.1 and v3 data
    time_vals_v21, flow_vals_v21 = ReadNWMv21Data(reach_id, start_time, end_time)
    time_vals_v3, flow_vals_v3 = ReadNWMv3Data(reach_id, start_time, end_time)

    # Retrieve USGS data for the same period
    time_vals_usgs, flow_vals_usgs = ReadUSGSData(
        usgs_station_id, '2020-08-24', '2020-09-03'
    )

    # Truncate NWM datasets for consistent plotting
    time_vals_v21_plot, flow_vals_v21_plot, time_vals_v3_plot, flow_vals_v3_plot = TruncateForPlotting(
        time_vals_v21, flow_vals_v21, time_vals_v3, flow_vals_v3
    )

    # Create the streamflow graph
    CreateStreamflowGraph(
        time_vals_v21_plot, flow_vals_v21_plot,
        time_vals_v3_plot, flow_vals_v3_plot,
        time_vals_usgs, flow_vals_usgs
    )
