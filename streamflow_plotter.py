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


def read_usgs_data(station_id, start_date, end_date):
    """
    Retrieve USGS data from the Water Services API and convert from cfs to cms.
    
    Parameters:
        station_id (str): USGS station ID.
        start_date (str): Start date in 'YYYY-MM-DD' format.
        end_date (str): End date in 'YYYY-MM-DD' format.
    
    Returns:
        tuple: (list of datetime objects, list of flow values in cms)
    
    Raises:
        ValueError: If no USGS data is available for the specified station and dates.
    """
    url = (
        f"https://waterservices.usgs.gov/nwis/iv/?format=json&sites={station_id}"
        f"&startDT={start_date}&endDT={end_date}&parameterCd=00060"
    )
    response = requests.get(url)
    data = response.json()

    # Check if timeSeries data is available
    if not data['value']['timeSeries']:
        raise ValueError(
            f"No USGS data available for station {station_id} from {start_date} to {end_date}"
        )

    flow_vals = []
    time_vals = []
    for value in data['value']['timeSeries'][0]['values'][0]['value']:
        flow_vals.append(float(value['value']) * CFS_TO_CMS)  # Convert cfs to cms
        time_vals.append(pd.to_datetime(value['dateTime']))

    return time_vals, flow_vals


def get_time_series_at_reach(store, reach_id, start_time_index, end_time_index):
    """
    Retrieve the streamflow time series for a specified reach ID from a data store.
    
    Parameters:
        store (xarray.Dataset): Dataset loaded from Zarr.
        reach_id (int): NWM reach ID.
        start_time_index (int): Starting index for the time dimension.
        end_time_index (int): Ending index for the time dimension.
    
    Returns:
        numpy.ndarray: Array of streamflow values for the specified reach.
    """
    streamflow_array = store['streamflow']
    feature_id_array = store['feature_id']
    flows = streamflow_array.where(feature_id_array == reach_id, drop=True)
    return flows[start_time_index:end_time_index].values


def get_time_array(store, start_time_index, end_time_index):
    """
    Get the time array from a data store.
    
    Parameters:
        store (xarray.Dataset): Dataset loaded from Zarr.
        start_time_index (int): Starting index for the time dimension.
        end_time_index (int): Ending index for the time dimension.
    
    Returns:
        numpy.ndarray: Array of datetime values.
    """
    time_array = store['time']
    return time_array[start_time_index:end_time_index].values


def read_nwm_v21_data(reach_id, start_time, end_time):
    """
    Read NWM v2.1 data from AWS without downloading the entire dataset.
    
    Parameters:
        reach_id (int): NWM reach ID.
        start_time (str): Start time in ISO format.
        end_time (str): End time in ISO format.
    
    Returns:
        tuple: (numpy array of time values, numpy array of streamflow data)
    """
    url = "s3://noaa-nwm-retrospective-2-1-zarr-pds/chrtout.zarr"
    fs = s3fs.S3FileSystem(anon=True)
    store = xr.open_zarr(s3fs.S3Map(url, s3=fs), consolidated=False)

    zero_start_time = np.datetime64('1979-02-01T00:00:00')
    start_time_index = int((np.datetime64(start_time) - zero_start_time) / np.timedelta64(1, 'h'))
    end_time_index = int((np.datetime64(end_time) - zero_start_time) / np.timedelta64(1, 'h'))

    time_series = get_time_series_at_reach(store, reach_id, start_time_index, end_time_index)
    time_vals = get_time_array(store, start_time_index, end_time_index)

    return time_vals, time_series


def read_nwm_v3_data(reach_id, start_time, end_time):
    """
    Read NWM v3 data from AWS without downloading the entire dataset.
    
    Parameters:
        reach_id (int): NWM reach ID.
        start_time (str): Start time in ISO format.
        end_time (str): End time in ISO format.
    
    Returns:
        tuple: (numpy array of time values, numpy array of streamflow data)
    """
    url = "s3://noaa-nwm-retrospective-3-0-pds/CONUS/zarr/chrtout.zarr"
    fs = s3fs.S3FileSystem(anon=True)
    store = xr.open_zarr(s3fs.S3Map(url, s3=fs), consolidated=False)

    zero_start_time = np.datetime64('1979-02-01T00:00:00')
    start_time_index = int((np.datetime64(start_time) - zero_start_time) / np.timedelta64(1, 'h'))
    end_time_index = int((np.datetime64(end_time) - zero_start_time) / np.timedelta64(1, 'h'))

    time_series = get_time_series_at_reach(store, reach_id, start_time_index, end_time_index)
    time_vals = get_time_array(store, start_time_index, end_time_index)

    return time_vals, time_series


def truncate_for_plotting(time_vals_v21, flow_vals_v21, time_vals_v3, flow_vals_v3):
    """
    Truncate the NWM v2.1 and v3 datasets so that they have the same length for plotting.
    
    Parameters:
        time_vals_v21 (array-like): Time values for NWM v2.1.
        flow_vals_v21 (array-like): Streamflow data for NWM v2.1.
        time_vals_v3 (array-like): Time values for NWM v3.
        flow_vals_v3 (array-like): Streamflow data for NWM v3.
    
    Returns:
        tuple: Truncated (time_vals_v21, flow_vals_v21, time_vals_v3, flow_vals_v3)
    """
    min_length = min(len(flow_vals_v21), len(flow_vals_v3))
    return (time_vals_v21[:min_length], flow_vals_v21[:min_length],
            time_vals_v3[:min_length], flow_vals_v3[:min_length])


def create_streamflow_graph(time_vals_v21, flow_vals_v21, time_vals_v3, flow_vals_v3,
                            time_vals_usgs, flow_vals_usgs):
    """
    Create and display a streamflow graph comparing NWM v2.1, NWM v3, and USGS data.
    
    Parameters:
        time_vals_v21 (array-like): Time values for NWM v2.1.
        flow_vals_v21 (array-like): Flow values for NWM v2.1.
        time_vals_v3 (array-like): Time values for NWM v3.
        flow_vals_v3 (array-like): Flow values for NWM v3.
        time_vals_usgs (array-like): Time values for USGS data.
        flow_vals_usgs (array-like): Flow values for USGS data.
    """
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
    plt.gca().xaxis.set_major_locator(mdates.DayLocator())

    plt.xlabel('Date (Daily)')
    plt.ylabel('Streamflow (cms)')
    plt.title('Daily Streamflow at Magnolia River, AL (NWMID#18516010)')
    plt.legend(loc='upper right')
    plt.xticks(rotation=45)
    plt.grid(True, linestyle='--')
    plt.tight_layout()
    plt.show()


def main():
    """
    Main function to execute streamflow data retrieval and plotting.
    """
    # Configuration
    reach_id = 18516010          # NWM reach ID
    usgs_station_id = "02378300" # USGS station ID for Buffalo Bayou
    start_time = '2020-08-24T00:00:00'
    end_time = '2020-09-03T23:59:59'

    # Retrieve NWM v2.1 and v3 data
    time_vals_v21, flow_vals_v21 = read_nwm_v21_data(reach_id, start_time, end_time)
    time_vals_v3, flow_vals_v3 = read_nwm_v3_data(reach_id, start_time, end_time)

    # Retrieve USGS data for the same period
    time_vals_usgs, flow_vals_usgs = read_usgs_data(usgs_station_id, '2020-08-24', '2020-09-03')

    # Truncate NWM datasets for consistent plotting
    (time_vals_v21_plot, flow_vals_v21_plot,
     time_vals_v3_plot, flow_vals_v3_plot) = truncate_for_plotting(
        time_vals_v21, flow_vals_v21, time_vals_v3, flow_vals_v3
    )

    # Create the streamflow graph
    create_streamflow_graph(time_vals_v21_plot, flow_vals_v21_plot,
                            time_vals_v3_plot, flow_vals_v3_plot,
                            time_vals_usgs, flow_vals_usgs)


if __name__ == "__main__":
    main()
