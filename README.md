# Streamflow Data Plotter

This repository contains a Python script that retrieves and compares streamflow data from two sources:

- **National Water Model (NWM)**
  - Retrieves v2.1 and v3 streamflow data directly from AWS using the Zarr format.
- **USGS**
  - Retrieves real-time streamflow data using the USGS Water Services API.

The script plots the streamflow time series from these sources on a single graph, allowing you to easily compare the datasets.

## Example Screenshot

Below is an example screenshot of the streamflow graph generated by the script:

![Streamflow Comparison](![image](https://github.com/user-attachments/assets/db2a3876-6f56-43ae-8ea1-c728b960b7c5))

*Make sure to place the `screenshot.png` image in the repository's root directory or update the path accordingly.*

## Features

- **Data Retrieval:**
  - Retrieves NWM data (v2.1 and v3) directly from AWS without downloading the entire dataset.
- **USGS Integration:**
  - Converts USGS streamflow data from cubic feet per second (cfs) to cubic meters per second (cms) for consistency.
- **Time Series Alignment:**
  - Truncates the datasets to ensure that all data series have the same length for accurate plotting.
- **Visualization:**
  - Uses Matplotlib to create a clear plot with daily tick intervals.

## Installation

1. **Clone the Repository:**
    ```bash
    git clone https://github.com/yourusername/streamflow-data-plotter.git
    cd streamflow-data-plotter
    ```

2. **Set Up a Virtual Environment (Optional but Recommended):**
    ```bash
    python3 -m venv venv
    source venv/bin/activate  # On Windows use `venv\Scripts\activate`
    ```

3. **Install Dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

   *Example `requirements.txt`:*
    ```
    xarray
    numpy
    s3fs
    matplotlib
    requests
    pandas
    ```

## Usage

Run the script directly:

```bash
python streamflow_plotter.py
