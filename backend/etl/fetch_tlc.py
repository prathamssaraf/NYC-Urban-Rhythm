#!/usr/bin/env python3
"""
Universal ETL script for fetching NYC TLC (Taxi & Limousine Commission) trip data 
from NYC Open Data for any year
"""

import os
import argparse
import logging
from datetime import datetime, timedelta
import requests
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from . import get_default_date_range, save_to_csv

# Load environment variables
load_dotenv()

# Set up logger
logger = logging.getLogger(__name__)

# Dataset ID mapping by year
YELLOW_TAXI_DATASET_IDS = {
    2009: "9hdn-4gtv",  # Dataset ID for 2009 Yellow Taxi data
    2010: "db5u-fvkr",  # Dataset ID for 2010 Yellow Taxi data
    2011: "uwyp-v8h4",  # Dataset ID for 2011 Yellow Taxi data
    2012: "cvic-zcjb",  # Dataset ID for 2012 Yellow Taxi data
    2013: "t7ny-aygi",  # Dataset ID for 2013 Yellow Taxi data
    2014: "gkne-dk5s",  # Dataset ID for 2014 Yellow Taxi data
    2015: "ba8s-jw6u",  # Dataset ID for 2015 Yellow Taxi data
    2016: "k67s-dv2t",  # Dataset ID for 2016 Yellow Taxi data
    2017: "biws-g3hs",  # Dataset ID for 2017 Yellow Taxi data
    2018: "t29m-gskq",  # Dataset ID for 2018 Yellow Taxi data
    2019: "2upf-qytp",  # Dataset ID for 2019 Yellow Taxi data
    2020: "kxp8-n2sj",  # Dataset ID for 2020 Yellow Taxi data
    2021: "m6nq-qud6",  # Dataset ID for 2021 Yellow Taxi data
    2022: "qp3b-zxtp",  # Dataset ID for 2022 Yellow Taxi data
    2023: "uwyp-trry",  # Dataset ID for 2023 Yellow Taxi data
    # Add more years as they become available
}

def get_dataset_id(year):
    """Get dataset ID for a specific year"""
    return YELLOW_TAXI_DATASET_IDS.get(year)

def fetch_tlc_data(year, month, limit=50000):
    """
    Fetch NYC TLC trip data from NYC Open Data API for any year
    
    Args:
        year: Year for data fetching
        month: Month for data fetching
        limit: Maximum number of records to fetch
        
    Returns:
        Pandas DataFrame with TLC data
    """
    logger.info(f"Fetching Yellow Taxi trip data for {year}-{month:02d}")
    
    # Get dataset ID for the specified year
    dataset_id = get_dataset_id(year)
    if not dataset_id:
        logger.error(f"No dataset ID available for year {year}")
        return pd.DataFrame()
    
    # Construct API endpoint
    api_endpoint = f"https://data.cityofnewyork.us/resource/{dataset_id}.json"
    
    # Format dates for query
    start_date = f"{year}-{month:02d}-01T00:00:00"
    
    # Calculate end date (last day of the month)
    if month == 12:
        end_date = f"{year+1}-01-01T00:00:00"
    else:
        end_date = f"{year}-{month+1:02d}-01T00:00:00"
    
    # App token for NYC Open Data
    app_token = os.getenv("NYC_OPEN_DATA_APP_TOKEN")
    
    # Set up headers with app token
    headers = {}
    if app_token:
        headers["X-App-Token"] = app_token
    
    # Handle different column naming across years
    # Pre-2016 data uses different column names for pickup/dropoff dates
    if year < 2016:
        pickup_date_col = "pickup_datetime"
        dropoff_date_col = "dropoff_datetime"
    else:
        pickup_date_col = "tpep_pickup_datetime"
        dropoff_date_col = "tpep_dropoff_datetime"
    
    # Parameters for API query
    params = {
        "$where": f"{pickup_date_col} >= '{start_date}' AND {pickup_date_col} < '{end_date}'",
        "$limit": limit
    }
    
    all_data = []
    offset = 0
    page_size = 10000  # Smaller page size for better reliability
    
    # Fetch data with pagination
    while True:
        current_params = params.copy()
        current_params["$offset"] = offset
        current_params["$limit"] = page_size
        
        try:
            logger.info(f"Fetching page with offset {offset}")
            response = requests.get(api_endpoint, headers=headers, params=current_params)
            response.raise_for_status()
            
            data = response.json()
            
            # If no more results, break the loop
            if not data:
                break
                
            all_data.extend(data)
            logger.info(f"Fetched {len(data)} records (total: {len(all_data)})")
            
            # If fewer results than page_size, we've reached the end
            if len(data) < page_size:
                break
                
            # Increment offset for next page
            offset += page_size
            
            # If we've reached the overall limit, stop
            if len(all_data) >= limit:
                all_data = all_data[:limit]
                break
                
        except Exception as e:
            logger.error(f"Error fetching TLC data: {e}")
            break
    
    # Convert to DataFrame
    if all_data:
        df = pd.DataFrame(all_data)
        logger.info(f"Total of {len(df)} TLC records fetched")
        return df
    else:
        logger.warning("No TLC data fetched")
        return pd.DataFrame()

def process_tlc_data(df, year):
    """
    Process TLC data: clean, add coordinates, etc.
    
    Args:
        df: Raw TLC data
        year: Year of data for column naming
        
    Returns:
        GeoDataFrame with processed TLC data
    """
    logger.info("Processing TLC trip data")
    
    # Check if dataframe is empty
    if df.empty:
        logger.warning("Empty dataframe, returning empty GeoDataFrame")
        return gpd.GeoDataFrame()
    
    # Handle different column naming across years
    if year < 2016:
        pickup_date_col = "pickup_datetime"
        dropoff_date_col = "dropoff_datetime"
    else:
        pickup_date_col = "tpep_pickup_datetime"
        dropoff_date_col = "tpep_dropoff_datetime"
    
    # Standardize column names (create consistent names regardless of source format)
    column_mapping = {
        pickup_date_col: "pickup_datetime",
        dropoff_date_col: "dropoff_datetime",
        "passenger_count": "passenger_count",
        "trip_distance": "trip_distance",
    }
    
    # Rename columns if they exist
    for old_col, new_col in column_mapping.items():
        if old_col in df.columns:
            df[new_col] = df[old_col]
    
    # Convert datetime columns
    datetime_cols = ["pickup_datetime", "dropoff_datetime"]
    for col in datetime_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    
    # Initialize gdf as a simple conversion to prevent the UnboundLocalError
    gdf = gpd.GeoDataFrame(df)
    
    # Handle location data based on year
    if year < 2016:
        # Pre-2016 data has direct coordinates
        if "pickup_longitude" in df.columns and "pickup_latitude" in df.columns:
            df = df.dropna(subset=["pickup_longitude", "pickup_latitude"])
            df["pickup_longitude"] = pd.to_numeric(df["pickup_longitude"], errors="coerce")
            df["pickup_latitude"] = pd.to_numeric(df["pickup_latitude"], errors="coerce")
            
            # Filter out invalid coordinates
            df = df[(df["pickup_latitude"] > 40.5) & (df["pickup_latitude"] < 41.0) & 
                   (df["pickup_longitude"] > -74.3) & (df["pickup_longitude"] < -73.7)]
            
            # Create pickup geometry
            geometry = [Point(xy) for xy in zip(df["pickup_longitude"], df["pickup_latitude"])]
            
            # Create GeoDataFrame
            gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")
            
            # Add dropoff geometry if available
            if "dropoff_longitude" in df.columns and "dropoff_latitude" in df.columns:
                df["dropoff_longitude"] = pd.to_numeric(df["dropoff_longitude"], errors="coerce")
                df["dropoff_latitude"] = pd.to_numeric(df["dropoff_latitude"], errors="coerce")
                
                # Create dropoff points for valid coordinates
                valid_dropoffs = (df["dropoff_latitude"] > 40.5) & (df["dropoff_latitude"] < 41.0) & \
                                 (df["dropoff_longitude"] > -74.3) & (df["dropoff_longitude"] < -73.7)
                
                gdf["dropoff_geometry"] = None
                for idx, row in gdf[valid_dropoffs].iterrows():
                    gdf.at[idx, "dropoff_geometry"] = Point(row["dropoff_longitude"], row["dropoff_latitude"])
    
    else:
        # Post-2016 data has location IDs instead of coordinates
        # We need to join with the taxi zones shapefile
        if "PULocationID" in df.columns:
            # Load taxi zone data (this would need to be implemented)
            # For now, we'll just use a placeholder
            logger.info("Using taxi zone IDs for geospatial data")
            
            # Convert to GeoDataFrame without geometry for now
            # In a full implementation, you would join with the taxi zones shapefile
            gdf = gpd.GeoDataFrame(df)
            
            # Flag that we need to join with zones later
            gdf.attrs["needs_zone_join"] = True
    
    # Add time components for multi-resolution analysis
    if "pickup_datetime" in gdf.columns:
        gdf["hour"] = gdf["pickup_datetime"].dt.hour
        gdf["day"] = gdf["pickup_datetime"].dt.day
        gdf["weekday"] = gdf["pickup_datetime"].dt.weekday
        gdf["month"] = gdf["pickup_datetime"].dt.month
        gdf["year"] = gdf["pickup_datetime"].dt.year
    
    logger.info(f"Processed {len(gdf)} TLC trip records")
    return gdf

def load_to_database(gdf, db_conn_string, year):
    """
    Load TLC data to PostgreSQL database
    
    Args:
        gdf: GeoDataFrame with TLC data
        db_conn_string: Database connection string
        year: Year of data being loaded
    """
    logger.info("Loading TLC data to database")
    
    # If dataframe is empty, don't try to load
    if gdf.empty:
        logger.warning("No data to load to database")
        return
    
    # Connect to PostgreSQL
    conn = psycopg2.connect(db_conn_string)
    cursor = conn.cursor()
    
    try:
        # Create temporary table for data import with matching schema
        create_temp_table_sql = """
        CREATE TEMP TABLE temp_tlc_trips (
            pickup_datetime TIMESTAMP,
            dropoff_datetime TIMESTAMP,
            passenger_count INTEGER,
            trip_distance FLOAT,
            pickup_longitude FLOAT,
            pickup_latitude FLOAT,
            dropoff_longitude FLOAT,
            dropoff_latitude FLOAT,
            pickup_locationid INTEGER,
            dropoff_locationid INTEGER,
            pickup_geom GEOMETRY(Point, 4326),
            dropoff_geom GEOMETRY(Point, 4326)
        ) ON COMMIT DROP
        """
        
        cursor.execute(create_temp_table_sql)
        
        # Prepare data for insert
        data = []
        
        # Check if we need to handle different schemas based on year
        has_coordinates = "geometry" in gdf.columns and gdf.geometry.notna().any()
        has_location_ids = "PULocationID" in gdf.columns and "DOLocationID" in gdf.columns
        
        for idx, row in gdf.iterrows():
            # Handle passenger count conversion properly
            try:
                passenger_count_val = row.get("passenger_count", 1)
                if pd.notnull(passenger_count_val):
                    # First convert to float, then to int to handle string representations of floats
                    passenger_count = int(float(passenger_count_val))
                else:
                    passenger_count = 1
            except (ValueError, TypeError):
                passenger_count = 1
            
            # Handle trip distance conversion
            try:
                trip_distance_val = row.get("trip_distance", 0)
                if pd.notnull(trip_distance_val):
                    trip_distance = float(trip_distance_val)
                else:
                    trip_distance = 0.0
            except (ValueError, TypeError):
                trip_distance = 0.0
            
            # Create a list of values for this row
            row_data = [
                row.get("pickup_datetime"),
                row.get("dropoff_datetime"),
                passenger_count,
                trip_distance,
                None,  # pickup_longitude
                None,  # pickup_latitude
                None,  # dropoff_longitude
                None,  # dropoff_latitude
                None,  # pickup_locationid
                None,  # dropoff_locationid
                None,  # pickup_geom
                None   # dropoff_geom
            ]
            
            # Add coordinates if available (pre-2016 data)
            if has_coordinates:
                if hasattr(row, "geometry") and row.geometry:
                    row_data[4] = row.geometry.x  # pickup_longitude
                    row_data[5] = row.geometry.y  # pickup_latitude
                    row_data[10] = f"SRID=4326;{row.geometry.wkt}"  # pickup_geom
                
                if hasattr(row, "dropoff_geometry") and row.dropoff_geometry:
                    row_data[6] = row.dropoff_geometry.x  # dropoff_longitude
                    row_data[7] = row.dropoff_geometry.y  # dropoff_latitude
                    row_data[11] = f"SRID=4326;{row.dropoff_geometry.wkt}"  # dropoff_geom
            
            # Add location IDs if available (post-2016 data)
            if has_location_ids:
                try:
                    if pd.notnull(row.get("PULocationID")):
                        row_data[8] = int(float(row.get("PULocationID")))
                except (ValueError, TypeError):
                    pass
                
                try:
                    if pd.notnull(row.get("DOLocationID")):
                        row_data[9] = int(float(row.get("DOLocationID")))
                except (ValueError, TypeError):
                    pass
            
            data.append(row_data)
        
        # Insert data into temp table
        insert_query = """
        INSERT INTO temp_tlc_trips (
            pickup_datetime, dropoff_datetime, passenger_count, trip_distance,
            pickup_longitude, pickup_latitude, dropoff_longitude, dropoff_latitude,
            pickup_locationid, dropoff_locationid, pickup_geom, dropoff_geom
        ) VALUES %s
        """
        execute_values(cursor, insert_query, data)
        
        # Insert into main table
        insert_sql = """
        INSERT INTO tlc_trips (
            pickup_datetime, dropoff_datetime, passenger_count, trip_distance,
            pickup_location, dropoff_location, pickup_neighborhood_id, dropoff_neighborhood_id
        )
        SELECT 
            t.pickup_datetime, t.dropoff_datetime, t.passenger_count, t.trip_distance,
            t.pickup_geom, t.dropoff_geom, 
            pickup_n.id, dropoff_n.id
        FROM temp_tlc_trips t
        LEFT JOIN neighborhoods pickup_n ON ST_Within(t.pickup_geom, pickup_n.geometry)
        LEFT JOIN neighborhoods dropoff_n ON ST_Within(t.dropoff_geom, dropoff_n.geometry)
        ON CONFLICT DO NOTHING
        """
        
        cursor.execute(insert_sql)
        
        # Commit transaction
        conn.commit()
        logger.info(f"Loaded {cursor.rowcount} TLC trip records to database")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error loading TLC data to database: {e}")
        raise
        
    finally:
        cursor.close()
        conn.close()


def main():
    """Main ETL function"""
    parser = argparse.ArgumentParser(description='Fetch NYC TLC yellow taxi trip data for any year')
    parser.add_argument('--year', type=int, default=datetime.now().year, help='Year of data to fetch')
    parser.add_argument('--month', type=int, default=datetime.now().month-1, help='Month of data to fetch')
    parser.add_argument('--save-csv', action='store_true', help='Save data to CSV')
    parser.add_argument('--limit', type=int, default=50000, help='Maximum number of records to fetch')
    args = parser.parse_args()
    
    # Validate year
    if args.year not in YELLOW_TAXI_DATASET_IDS:
        logger.error(f"No dataset available for year {args.year}")
        available_years = ', '.join(str(y) for y in sorted(YELLOW_TAXI_DATASET_IDS.keys()))
        logger.info(f"Available years: {available_years}")
        return
    
    # Adjust month if needed
    if args.month == 0:
        args.month = 12
        args.year -= 1
    
    # Fetch and process data
    df = fetch_tlc_data(args.year, args.month, args.limit)
    if not df.empty:
        gdf = process_tlc_data(df, args.year)
        
        # Save to CSV if requested
        if args.save_csv:
            date_str = f"{args.year}{args.month:02d}"
            save_to_csv(df, f'yellow_taxi_{date_str}', date_str)
        
        # Load to database
        db_conn_string = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"
        load_to_database(gdf, db_conn_string, args.year)
    
    logger.info("TLC ETL process completed successfully")

if __name__ == "__main__":
    main()