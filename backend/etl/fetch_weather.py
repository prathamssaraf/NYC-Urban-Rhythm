#!/usr/bin/env python3
"""
ETL script for fetching NYC weather data using NOAA API
"""

import os
import argparse
import logging
from datetime import datetime, timedelta
import requests
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from . import get_default_date_range, save_to_csv

# Load environment variables
load_dotenv()

# Set up logger
logger = logging.getLogger(__name__)

def fetch_weather_data(start_date, end_date):
    """
    Fetch NYC weather data using NOAA API
    
    Args:
        start_date: Start date for data fetching
        end_date: End date for data fetching
        
    Returns:
        Pandas DataFrame with weather data
    """
    logger.info(f"Fetching weather data from {start_date} to {end_date}")
    
    # NOAA API token
    token = os.getenv("NOAA_API_TOKEN")
    if not token:
        logger.error("No NOAA API token found in environment variables")
        return pd.DataFrame()
    
    # Headers for API request
    headers = {
        "token": token
    }
    
    # Format dates for query
    start_str = start_date.strftime('%Y-%m-%d')
    end_str = end_date.strftime('%Y-%m-%d')
    
    # Central Park Station ID (GHCND:USW00094728)
    station_id = "GHCND:USW00094728"
    
    # API endpoint
    url = f"https://www.ncdc.noaa.gov/cdo-web/api/v2/data"
    
    # Parameters for API query
    params = {
        "datasetid": "GHCND",
        "stationid": station_id,
        "startdate": start_str,
        "enddate": end_str,
        "units": "standard",
        "limit": 1000,
        "datatypeid": "TMAX,TMIN,PRCP,AWND",
        "includemetadata": "false"
    }
    
    all_data = []
    offset = 1
    
    # Fetch data with pagination
    while True:
        current_params = params.copy()
        current_params["offset"] = offset
        
        try:
            logger.info(f"Fetching page with offset {offset}")
            response = requests.get(url, headers=headers, params=current_params)
            response.raise_for_status()
            
            data = response.json()
            
            # If no results, break the loop
            if "results" not in data or not data["results"]:
                break
                
            all_data.extend(data["results"])
            logger.info(f"Fetched {len(data['results'])} records (total: {len(all_data)})")
            
            # If fewer results than limit, we've reached the end
            if len(data["results"]) < params["limit"]:
                break
                
            # Increment offset for next page
            offset += params["limit"]
                
        except Exception as e:
            logger.error(f"Error fetching weather data: {e}")
            break
    
    # Process the data into a more usable format
    processed_data = []
    
    # Group data by date
    dates = {}
    for item in all_data:
        date = item["date"].split("T")[0]
        if date not in dates:
            dates[date] = {}
        
        # Store each data type with its value
        dates[date][item["datatype"]] = item["value"]
    
    # Create records for each date
    for date, values in dates.items():
        record = {
            "datetime": datetime.strptime(date, "%Y-%m-%d"),
            "temperature": (values.get("TMAX", 0) + values.get("TMIN", 0)) / 2 if "TMAX" in values and "TMIN" in values else None,
            "precipitation": values.get("PRCP"),
            "wind_speed": values.get("AWND"),
            "weather_condition": None  # NOAA doesn't provide a simple weather condition
        }
        processed_data.append(record)
    
    # Convert to DataFrame
    if processed_data:
        df = pd.DataFrame(processed_data)
        logger.info(f"Total of {len(df)} weather records fetched")
        return df
    else:
        logger.warning("No weather data fetched")
        return pd.DataFrame()

def process_weather_data(df):
    """
    Process weather data: clean, convert units, etc.
    
    Args:
        df: Raw weather data DataFrame
        
    Returns:
        DataFrame with processed weather data
    """
    logger.info("Processing weather data")
    
    # Check if DataFrame is empty
    if df.empty:
        logger.warning("Empty dataframe, no data to process")
        return df
    
    # Add time components for multi-resolution analysis
    df['hour'] = df['datetime'].dt.hour
    df['day'] = df['datetime'].dt.day
    df['weekday'] = df['datetime'].dt.weekday
    df['month'] = df['datetime'].dt.month
    df['year'] = df['datetime'].dt.year
    
    logger.info(f"Processed {len(df)} weather records")
    return df

def load_to_database(df, db_conn_string):
    """
    Load weather data to PostgreSQL database
    
    Args:
        df: DataFrame with weather data
        db_conn_string: Database connection string
    """
    logger.info("Loading weather data to database")
    
    # If DataFrame is empty, don't try to load
    if df.empty:
        logger.warning("No data to load to database")
        return
    
    # Connect to PostgreSQL
    conn = psycopg2.connect(db_conn_string)
    cursor = conn.cursor()
    
    try:
        # Prepare data for insert
        data = []
        for idx, row in df.iterrows():
            # Create a record for this row
            data.append((
                row['datetime'],
                float(row.get('temperature', 0)) if pd.notnull(row.get('temperature', 0)) else None,
                float(row.get('precipitation', 0)) if pd.notnull(row.get('precipitation', 0)) else None,
                None,  # humidity (not available in NOAA data)
                float(row.get('wind_speed', 0)) if pd.notnull(row.get('wind_speed', 0)) else None,
                row.get('weather_condition')
            ))
        
        # First, check if the datetime column has a unique constraint
        cursor.execute("""
        SELECT COUNT(*) 
        FROM pg_constraint 
        WHERE conrelid = 'weather'::regclass::oid 
        AND contype = 'u' 
        AND conkey @> ARRAY[
            (SELECT attnum FROM pg_attribute WHERE attrelid = 'weather'::regclass AND attname = 'datetime')
        ]::smallint[]
        """)
        
        has_unique_constraint = cursor.fetchone()[0] > 0
        
        if has_unique_constraint:
            # If there's a unique constraint, use ON CONFLICT
            insert_query = """
            INSERT INTO weather (
                datetime, temperature, precipitation, humidity, wind_speed, weather_condition
            ) VALUES %s
            ON CONFLICT (datetime) DO UPDATE SET
                temperature = EXCLUDED.temperature,
                precipitation = EXCLUDED.precipitation,
                wind_speed = EXCLUDED.wind_speed
            """
        else:
            # If there's no unique constraint, use a simpler approach
            # First delete any records with the same datetime, then insert
            for row in data:
                cursor.execute("""
                DELETE FROM weather 
                WHERE datetime = %s
                """, (row[0],))
            
            # Then insert new records
            insert_query = """
            INSERT INTO weather (
                datetime, temperature, precipitation, humidity, wind_speed, weather_condition
            ) VALUES %s
            """
        
        execute_values(cursor, insert_query, data)
        
        # Commit transaction
        conn.commit()
        logger.info(f"Loaded {cursor.rowcount} weather records to database")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error loading weather data to database: {e}")
        raise
        
    finally:
        cursor.close()
        conn.close()

def main():
    """Main ETL function"""
    parser = argparse.ArgumentParser(description='Fetch NYC weather data')
    parser.add_argument('--start-date', type=str, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, help='End date (YYYY-MM-DD)')
    parser.add_argument('--save-csv', action='store_true', help='Save data to CSV')
    args = parser.parse_args()
    
    # Get date range
    if args.start_date and args.end_date:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d')
    else:
        start_date, end_date = get_default_date_range()
    
    # Fetch and process data
    df = fetch_weather_data(start_date, end_date)
    df = process_weather_data(df)
    
    # Save to CSV if requested
    if args.save_csv and not df.empty:
        date_str = start_date.strftime('%Y%m%d') + '_' + end_date.strftime('%Y%m%d')
        save_to_csv(df, 'weather', date_str)
    
    # Load to database
    db_conn_string = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"
    load_to_database(df, db_conn_string)
    
    logger.info("Weather ETL process completed successfully")

if __name__ == "__main__":
    main()