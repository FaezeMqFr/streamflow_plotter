# Streamflow Data Plotter

This repository contains a Python script that retrieves and compares streamflow data from two sources:

- **National Water Model (NWM)**
  - Retrieves v2.1 and v3 streamflow data directly from AWS using the Zarr format.
- **USGS**
  - Retrieves real-time streamflow data using the USGS Water Services API.

The script plots the streamflow time series from these sources on a single graph, making it easy to visually compare the datasets.

## Example Screenshot

Below is a sample output showing daily streamflow data at a specific location:

![Daily Streamflow](![myplot](https://github.com/user-attachments/assets/144a11e6-1897-4f77-8d54-a410b93f2504))

*(Replace `plot_screenshot.png` with the actual file name if it’s different.)*

## Features

- **Data Retrieval:**
  - Retrieves NWM data (v2.1 and v3) directly from AWS without downloading the entire dataset.
- **USGS Integration:**
  - Converts USGS streamflow data from cubic feet per second (cfs) to cubic meters per second (cms).
- **Time Series Alignment:**
  - Truncates datasets to ensure that all data series have the same length for consistent plotting.
- **Visualization:**
  - Uses Matplotlib to produce a clear plot with daily tick intervals.

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

   **Example `requirements.txt`**:
    ```
    xarray
    numpy
    s3fs
    matplotlib
    requests
    pandas
    ```

## Usage

Run the script:

```bash
python streamflow_plotter.py
