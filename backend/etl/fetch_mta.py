#!/usr/bin/env python3
"""
ETL script for fetching MTA subway ridership data from data.ny.gov
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

def fetch_mta_data(start_date, end_date, limit=50000):
    """
    Fetch MTA subway ridership data using the Socrata Open Data API
    
    Args:
        start_date: Start date for data fetching
        end_date: End date for data fetching
        limit: Maximum number of records to fetch
        
    Returns:
        Pandas DataFrame with MTA data
    """
    logger.info(f"Fetching MTA ridership data from {start_date} to {end_date}")
    
    # Format dates for query
    start_str = start_date.strftime('%Y-%m-%dT00:00:00')
    end_str = end_date.strftime('%Y-%m-%dT23:59:59')
    
    # Build API endpoint with query parameters
    api_endpoint = "https://data.ny.gov/resource/wujg-7c2s.json"
    
    # Parameters for the API request
    # Using SoQL (Socrata Query Language) to filter data
    params = {
        "$where": f"transit_timestamp >= '{start_str}' AND transit_timestamp <= '{end_str}'",
        "$limit": limit,
        "$order": "transit_timestamp ASC"
    }
    
    # Add app token if available
    headers = {}
    if os.getenv("SOCRATA_APP_TOKEN"):
        headers["X-App-Token"] = os.getenv("SOCRATA_APP_TOKEN")
    
    # Fetch data
    try:
        response = requests.get(api_endpoint, params=params, headers=headers)
        response.raise_for_status()  # Raise exception for 4XX/5XX responses
        
        data = response.json()
        logger.info(f"Fetched {len(data)} MTA ridership records")
        
        # Convert to DataFrame
        df = pd.DataFrame(data)
        
        # Convert timestamp to datetime
        if 'transit_timestamp' in df.columns:
            df['transit_timestamp'] = pd.to_datetime(df['transit_timestamp'])
            
        # Convert numeric columns
        if 'ridership' in df.columns:
            df['ridership'] = pd.to_numeric(df['ridership'])
            
        return df
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching MTA data: {e}")
        raise

def process_mta_data(df):
    """
    Process MTA data: clean, add coordinates, restructure data
    
    Args:
        df: Raw MTA ridership data
        
    Returns:
        GeoDataFrame with processed MTA data
    """
    logger.info("Processing MTA ridership data")
    
    # Make a copy to avoid SettingWithCopyWarning
    df = df.copy()
    
    # Filter out rows without required data
    df = df.dropna(subset=['station_complex', 'transit_timestamp', 'ridership'])
    
    # Create a mapping of station names to coordinates
    # This would ideally come from another dataset or a lookup table
    # For this example, we'll create a simple dictionary with a few station coordinates
    station_locations = {
        'Grand Central-42 St': (40.7527, -73.9772),
        'Times Sq-42 St': (40.7557, -73.9874),
        'Union Sq-14 St': (40.7356, -73.9906),
        '34 St-Herald Sq': (40.7497, -73.9877),
        '59 St-Columbus Circle': (40.7678, -73.9826),
        # Add more stations as needed
    }
    
    # Extract station name from station_complex field
    # This assumes station_complex is in a format like "Times Sq-42 St (N,Q,R,W,S,1,2,3,7)"
    # We need to handle various formats, so this is a simplified approach
    df['station_name'] = df['station_complex'].apply(
        lambda x: x.split('(')[0].strip() if '(' in x else x
    )
    
    # Add coordinates to dataframe based on station name
    df['latitude'] = df['station_name'].apply(
        lambda x: next((coords[0] for station, coords in station_locations.items() 
                       if station in x), None)
    )
    df['longitude'] = df['station_name'].apply(
        lambda x: next((coords[1] for station, coords in station_locations.items() 
                        if station in x), None)
    )
    
    # Filter out stations without coordinates
    df = df.dropna(subset=['latitude', 'longitude'])
    
    # Create geometry column
    geometry = [Point(xy) for xy in zip(df['longitude'], df['latitude'])]
    
    # Create GeoDataFrame
    gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")
    
    # Add time components for multi-resolution analysis
    gdf['hour'] = gdf['transit_timestamp'].dt.hour
    gdf['day'] = gdf['transit_timestamp'].dt.day
    gdf['weekday'] = gdf['transit_timestamp'].dt.weekday
    gdf['month'] = gdf['transit_timestamp'].dt.month
    gdf['year'] = gdf['transit_timestamp'].dt.year
    
    logger.info(f"Processed {len(gdf)} MTA ridership records")
    return gdf

def load_to_database(gdf, db_conn_string):
    """
    Load MTA data to PostgreSQL database
    
    Args:
        gdf: GeoDataFrame with MTA data
        db_conn_string: Database connection string
    """
    logger.info("Loading MTA data to database")
    
    # Connect to PostgreSQL
    conn = psycopg2.connect(db_conn_string)
    cursor = conn.cursor()
    
    try:
        # Create temporary table for data import
        cursor.execute("""
        CREATE TEMP TABLE temp_mta_turnstile (
            station_name VARCHAR(255),
            datetime TIMESTAMP,
            entries INTEGER,
            exits INTEGER,
            geom GEOMETRY(Point, 4326)
        ) ON COMMIT DROP
        """)
        
        # Prepare data for insert
        data = []
        for idx, row in gdf.iterrows():
            # We're converting ridership to entries for compatibility with existing schema
            data.append((
                row['station_name'],
                row['transit_timestamp'],
                int(row['ridership']),  # Use as "entries" in existing schema
                0,  # No exits data in the new dataset
                f"SRID=4326;{row.geometry.wkt}"
            ))
        
        # Insert data into temp table
        insert_query = """
        INSERT INTO temp_mta_turnstile (
            station_name, datetime, entries, exits, geom
        ) VALUES %s
        """
        execute_values(cursor, insert_query, data)
        
        # Insert into main table with neighborhood lookup
        cursor.execute("""
        INSERT INTO mta_turnstile (
            station_name, datetime, entries, exits, geometry, neighborhood_id
        )
        SELECT 
            t.station_name, t.datetime, t.entries, t.exits, t.geom, n.id
        FROM temp_mta_turnstile t
        LEFT JOIN neighborhoods n ON ST_Within(t.geom, n.geometry)
        """)
        
        # Commit transaction
        conn.commit()
        logger.info(f"Loaded {cursor.rowcount} MTA ridership records to database")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error loading MTA data to database: {e}")
        raise
        
    finally:
        cursor.close()
        conn.close()

def main():
    """Main ETL function"""
    parser = argparse.ArgumentParser(description='Fetch MTA subway ridership data')
    parser.add_argument('--start-date', type=str, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, help='End date (YYYY-MM-DD)')
    parser.add_argument('--limit', type=int, default=50000, help='Maximum number of records')
    parser.add_argument('--save-csv', action='store_true', help='Save data to CSV')
    parser.add_argument('--dry-run', action='store_true', help='Skip database insertion')
    parser.add_argument('--verbose', action='store_true', help='Show verbose output')
    args = parser.parse_args()
    
    # Set up logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Get date range
    if args.start_date and args.end_date:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d')
    else:
        start_date, end_date = get_default_date_range()
    
    try:
        # Fetch data
        df = fetch_mta_data(start_date, end_date, args.limit)
        
        if df.empty:
            logger.warning("No data was fetched. Skipping processing and database loading.")
            return
        
        # Process data
        gdf = process_mta_data(df)
        
        # Save to CSV if requested
        if args.save_csv and not df.empty:
            date_str = start_date.strftime('%Y%m%d') + '_' + end_date.strftime('%Y%m%d')
            save_to_csv(df, 'mta', date_str)
        
        # Load to database if not in dry-run mode
        if not args.dry_run and not gdf.empty:
            db_conn_string = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"
            load_to_database(gdf, db_conn_string)
            logger.info("MTA ETL process completed successfully")
        elif args.dry_run:
            logger.info("Dry run completed, skipping database insertion")
        
    except Exception as e:
        logger.error(f"Error in MTA ETL process: {e}")
        if args.verbose:
            import traceback
            logger.error(traceback.format_exc())

if __name__ == "__main__":
    main()