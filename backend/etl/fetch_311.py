#!/usr/bin/env python3
"""
ETL script for fetching NYC 311 service request data directly from API endpoint
"""

import os
import argparse
import logging
import requests
from datetime import datetime, timedelta
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

def fetch_311_data(start_date, end_date, limit=50000):
    """
    Fetch NYC 311 service request data using direct API endpoint
    
    Args:
        start_date: Start date for data fetching
        end_date: End date for data fetching
        limit: Maximum number of records to fetch
        
    Returns:
        Pandas DataFrame with 311 data
    """
    logger.info(f"Fetching 311 data from {start_date} to {end_date}")
    
    # Format dates for query
    start_str = start_date.strftime('%Y-%m-%dT00:00:00')
    end_str = end_date.strftime('%Y-%m-%dT23:59:59')
    
    # Build API endpoint with query parameters
    api_endpoint = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
    
    # Parameters for the API request
    params = {
        "$where": f"created_date between '{start_str}' and '{end_str}'",
        "$limit": limit,
        "$select": "unique_key, created_date, closed_date, agency, complaint_type, descriptor, location_type, incident_zip, incident_address, street_name, city, borough, latitude, longitude, location"
    }
    
    # Add app token if available
    headers = {}
    if os.getenv("NYC_OPEN_DATA_APP_TOKEN"):
        # Use the X-App-Token header instead of query parameter
        headers["X-App-Token"] = os.getenv("NYC_OPEN_DATA_APP_TOKEN")
    
    # Fetch data
    try:
        response = requests.get(api_endpoint, params=params, headers=headers)
        response.raise_for_status()  # Raise exception for 4XX/5XX responses
        
        results = response.json()
        logger.info(f"Fetched {len(results)} 311 records")
        
        # Convert to DataFrame
        df = pd.DataFrame(results)
        
        # Convert date strings to datetime objects
        if 'created_date' in df.columns:
            df['created_date'] = pd.to_datetime(df['created_date'])
        if 'closed_date' in df.columns:
            df['closed_date'] = pd.to_datetime(df['closed_date'])
            
        return df
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching 311 data: {e}")
        raise

def process_311_data(df):
    """
    Process 311 data: clean, add coordinates, etc.
    
    Args:
        df: Raw 311 data DataFrame
        
    Returns:
        GeoDataFrame with processed 311 data
    """
    logger.info("Processing 311 data")
    
    # Create a copy of the DataFrame to avoid the SettingWithCopyWarning
    df = df.copy()
    
    # Drop rows with missing coordinates
    df = df.dropna(subset=['latitude', 'longitude'])
    
    # Convert to numeric
    df['latitude'] = pd.to_numeric(df['latitude'])
    df['longitude'] = pd.to_numeric(df['longitude'])
    
    # Create geometry column
    geometry = [Point(xy) for xy in zip(df['longitude'], df['latitude'])]
    
    # Create GeoDataFrame
    gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")
    
    # Add time components for multi-resolution analysis
    gdf['hour'] = gdf['created_date'].dt.hour
    gdf['day'] = gdf['created_date'].dt.day
    gdf['weekday'] = gdf['created_date'].dt.weekday
    gdf['month'] = gdf['created_date'].dt.month
    gdf['year'] = gdf['created_date'].dt.year
    
    logger.info(f"Processed {len(gdf)} 311 records")
    return gdf
def load_to_database(gdf, db_conn_string):
    """
    Load 311 data to PostgreSQL database
    
    Args:
        gdf: GeoDataFrame with 311 data
        db_conn_string: Database connection string
    """
    logger.info("Loading 311 data to database")
    
    # Connect to PostgreSQL
    conn = psycopg2.connect(db_conn_string)
    cursor = conn.cursor()
    
    try:
        # Create temporary table for data import
        cursor.execute("""
        CREATE TEMP TABLE temp_311_calls (
            created_date TIMESTAMP,
            complaint_type VARCHAR(255),
            descriptor VARCHAR(255),
            incident_zip VARCHAR(10),
            geom GEOMETRY(Point, 4326)
        ) ON COMMIT DROP
        """)
        
        # Prepare data for insert
        data = []
        for idx, row in gdf.iterrows():
            data.append((
                row.get('created_date'),
                row.get('complaint_type', ''),  # Provide empty string as default if missing
                row.get('descriptor', ''),      # Provide empty string as default if missing
                row.get('incident_zip', ''),    # Provide empty string as default if missing
                f"SRID=4326;{row.geometry.wkt}"
            ))
        
        # Insert data into temp table
        insert_query = """
        INSERT INTO temp_311_calls (
            created_date, complaint_type, descriptor, incident_zip, geom
        ) VALUES %s
        """
        execute_values(cursor, insert_query, data)
        
        # Insert into main table with neighborhood lookup
        cursor.execute("""
        INSERT INTO nyc_311_calls (
            created_date, complaint_type, descriptor, incident_zip, geometry, neighborhood_id
        )
        SELECT 
            t.created_date, t.complaint_type, t.descriptor, t.incident_zip, t.geom, n.id
        FROM temp_311_calls t
        LEFT JOIN neighborhoods n ON ST_Within(t.geom, n.geometry)
        """)
        
        # Commit transaction
        conn.commit()
        logger.info(f"Loaded {cursor.rowcount} 311 records to database")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error loading 311 data to database: {e}")
        raise
        
    finally:
        cursor.close()
        conn.close()

def main():
    """Main ETL function"""
    parser = argparse.ArgumentParser(description='Fetch NYC 311 service request data')
    parser.add_argument('--start-date', type=str, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, help='End date (YYYY-MM-DD)')
    parser.add_argument('--limit', type=int, default=50000, help='Maximum number of records')
    parser.add_argument('--save-csv', action='store_true', help='Save data to CSV')
    args = parser.parse_args()
    
    # Get date range
    if args.start_date and args.end_date:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d')
    else:
        start_date, end_date = get_default_date_range()
    
    # Fetch and process data
    df = fetch_311_data(start_date, end_date, args.limit)
    gdf = process_311_data(df)
    
    # Save to CSV if requested
    if args.save_csv:
        date_str = start_date.strftime('%Y%m%d') + '_' + end_date.strftime('%Y%m%d')
        save_to_csv(df, '311', date_str)
    
    # Load to database
    db_conn_string = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"
    load_to_database(gdf, db_conn_string)
    
    logger.info("311 ETL process completed successfully")

if __name__ == "__main__":
    main()